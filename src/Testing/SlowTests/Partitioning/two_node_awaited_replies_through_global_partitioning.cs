using System.Collections.Concurrent;
using IntegrationTests;
using JasperFx;
using JasperFx.Core;
using Marten;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Shouldly;
using Wolverine;
using Wolverine.Marten;
using Wolverine.RabbitMQ;
using Wolverine.Runtime;
using Wolverine.Runtime.Partitioning;
using Wolverine.Runtime.RemoteInvocation;
using Wolverine.Transports;
using Xunit;

namespace SlowTests.Partitioning;

/// <summary>
/// Awaited typed replies across a real cluster: two nodes sharing one RabbitMQ topology and one Postgres
/// message store, with partition ownership split between them by Wolverine's agent assignment. Requests
/// are deliberately sent to a group id whose slot the CALLER DOES NOT OWN, which is what proves the
/// request crossed the broker, executed on the partition owner, and the reply came back to the caller.
/// The in-process counterpart is <c>awaited_replies_through_global_partitioning</c> in CoreTests.
/// </summary>
[Collection("partitioning")]
public class two_node_awaited_replies_through_global_partitioning : IAsyncLifetime
{
    private const string BaseName = "twonode";
    private const int SlotCount = 5;
    private const string SchemaName = "twonode_partitioning";

    private IHost _nodeA = null!;
    private IHost _nodeB = null!;

    public async ValueTask InitializeAsync()
    {
        await using (var conn = new NpgsqlConnection(Servers.PostgresConnectionString))
        {
            await conn.OpenAsync();
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"DROP SCHEMA IF EXISTS {SchemaName} CASCADE;";
            await cmd.ExecuteNonQueryAsync();
        }

        LedgerHandler.Reset();

        // Started sequentially so the first node creates the Marten schema without a DDL race
        _nodeA = await buildHost("node-a").StartAsync();
        _nodeB = await buildHost("node-b").StartAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await _nodeA.StopAsync();
        await _nodeB.StopAsync();
        _nodeA.Dispose();
        _nodeB.Dispose();
    }

    private static IHostBuilder buildHost(string nodeName)
    {
        return Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                // Balanced, NOT Solo: under Solo a single node owns every exclusive listener and the
                // local shortcut answers every request in process, which is exactly the path this test
                // exists to avoid.
                opts.ServiceName = "TwoNodePartitioning";
                opts.Durability.Mode = DurabilityMode.Balanced;

                opts.UseRabbitMq().AutoProvision().AutoPurgeOnStartup();

                opts.Discovery.DisableConventionalDiscovery().IncludeType(typeof(LedgerHandler));

                opts.Services.AddMarten(m =>
                {
                    m.Connection(Servers.PostgresConnectionString);
                    m.DatabaseSchemaName = SchemaName;
                    m.DisableNpgsqlLogging = true;
                }).IntegrateWithWolverine();

                opts.MessagePartitioning.ByMessage<PostEntry>(x => x.AccountId);
                opts.MessagePartitioning.ByMessage<PostBadEntry>(x => x.AccountId);
                opts.MessagePartitioning.ByMessage<NestedEntry>(x => x.AccountId);

                opts.MessagePartitioning.GlobalPartitioned(topology =>
                {
                    topology.UseShardedRabbitQueues(BaseName, SlotCount);
                    topology.Message<PostEntry>();
                    topology.Message<PostBadEntry>();
                    topology.Message<NestedEntry>();
                });

                opts.Services.AddSingleton(new NodeName(nodeName));
            });
    }

    [Fact]
    public async Task reply_returns_to_the_caller_when_another_node_owns_the_partition()
    {
        var owners = await waitForStablePartitionOwnershipAsync();

        // Deliberately choose a group id whose slot node B owns, and invoke it from node A. Picking one
        // at random and hoping it crosses is what makes tests like this flaky.
        var accountId = findAccountIdOwnedBy(_nodeB, owners);

        var reply = await _nodeA.MessageBus()
            .InvokeAsync<EntryPosted>(new PostEntry(accountId, 42),
                new DeliveryOptions { InvokeThroughRouting = true }, CancellationToken.None, 60.Seconds());

        reply.AccountId.ShouldBe(accountId);
        reply.Amount.ShouldBe(42);
        reply.HandledBy.ShouldBe("node-b",
            "The command must execute on the node that owns the partition, not on the caller");
    }

    [Fact]
    public async Task one_group_stays_sequential_on_its_owner_across_both_nodes()
    {
        var owners = await waitForStablePartitionOwnershipAsync();
        var accountId = findAccountIdOwnedBy(_nodeB, owners);

        LedgerHandler.Reset();

        // Both nodes invoke the same group id concurrently. Every one of them has to end up on node B,
        // one at a time.
        var callers = new[] { _nodeA, _nodeB, _nodeA, _nodeB, _nodeA, _nodeB };
        var replies = await Task.WhenAll(callers.Select((host, i) => host.MessageBus()
            .InvokeAsync<EntryPosted>(new PostEntry(accountId, i),
                new DeliveryOptions { InvokeThroughRouting = true }, CancellationToken.None, 60.Seconds())));

        replies.Length.ShouldBe(callers.Length);
        replies.Select(x => x.HandledBy).Distinct().ShouldHaveSingleItem().ShouldBe("node-b");
        LedgerHandler.MaxConcurrency.ShouldBe(1,
            "Messages sharing a group id must never execute concurrently, even when invoked from different nodes");
    }

    [Fact]
    public async Task a_failure_on_the_owning_node_surfaces_as_a_request_reply_exception()
    {
        var owners = await waitForStablePartitionOwnershipAsync();
        var accountId = findAccountIdOwnedBy(_nodeB, owners);

        // The assertion that separates a real failure reply from "the caller gave up": this must throw
        // WolverineRequestReplyException well inside the 60s window, not TimeoutException at the end.
        var ex = await Should.ThrowAsync<WolverineRequestReplyException>(() => _nodeA.MessageBus()
            .InvokeAsync<EntryPosted>(new PostBadEntry(accountId),
                new DeliveryOptions { InvokeThroughRouting = true }, CancellationToken.None, 60.Seconds()));

        ex.Message.ShouldContain("this entry cannot be posted");
    }

    [Fact]
    public async Task refuses_direct_lane_reentry_on_the_owning_node_for_the_same_group_id()
    {
        // Delivered to node B over the broker, so Envelope.Destination names the external slot listener
        // while the handler executes on the companion local queue's lane. The nested invoke takes the
        // local shortcut to that same companion queue, so only slot identity can see the re-entry.
        var owners = await waitForStablePartitionOwnershipAsync();
        var accountId = findAccountIdOwnedBy(_nodeB, owners);

        await _nodeA.MessageBus().InvokeAsync<EntryPosted>(new PostEntry(accountId, 1, accountId),
            new DeliveryOptions { InvokeThroughRouting = true }, CancellationToken.None, 120.Seconds());

        assertRefusedImmediately();
    }

    [Fact]
    public async Task refuses_direct_lane_reentry_on_the_owning_node_for_a_different_group_id_on_the_same_lane()
    {
        // Two unrelated aggregates that happen to share a lane -- the case a guard keyed on group id
        // equality would miss
        var owners = await waitForStablePartitionOwnershipAsync();
        var accountId = findAccountIdOwnedBy(_nodeB, owners);
        var sameLane = findSameLaneCompanion(accountId);

        sameLane.ShouldNotBe(accountId);

        await _nodeA.MessageBus().InvokeAsync<EntryPosted>(new PostEntry(accountId, 1, sameLane),
            new DeliveryOptions { InvokeThroughRouting = true }, CancellationToken.None, 120.Seconds());

        assertRefusedImmediately();
    }

    [Fact]
    public async Task allows_a_nested_invoke_onto_a_different_lane_of_the_same_queue()
    {
        var owners = await waitForStablePartitionOwnershipAsync();
        var accountId = findAccountIdOwnedBy(_nodeB, owners);
        var otherLane = findDifferentLaneOnTheSameSlot(accountId);

        // Same companion queue, one lane over. A different slot would be a different queue entirely, and
        // the guard could never have fired for it.
        laneOf(otherLane).Slot.ShouldBe(laneOf(accountId).Slot);
        laneOf(otherLane).Lane.ShouldNotBe(laneOf(accountId).Lane);

        await _nodeA.MessageBus().InvokeAsync<EntryPosted>(new PostEntry(accountId, 1, otherLane),
            new DeliveryOptions { InvokeThroughRouting = true }, CancellationToken.None, 120.Seconds());

        LedgerHandler.NestedOutcome.ShouldBe("succeeded",
            $"A nested invoke onto a different lane must still work. Got: {LedgerHandler.NestedOutcome} / {LedgerHandler.NestedMessage}");
    }

    private static void assertRefusedImmediately()
    {
        LedgerHandler.NestedOutcome.ShouldBe(nameof(InvalidOperationException),
            $"The nested invoke should have been refused outright. Got: {LedgerHandler.NestedOutcome} / {LedgerHandler.NestedMessage}");
        LedgerHandler.NestedMessage.ShouldNotBeNull().ShouldContain("same partitioned lane");

        // A deadlock that resolves by timing out also throws, just 30 seconds later
        LedgerHandler.NestedElapsed.ShouldBeLessThan(5.Seconds(),
            "The refusal must be immediate, not the nested invocation timing out");
    }

    /// <summary>
    /// Poll each node's listening agents until partition ownership is usable and settled, with a bounded
    /// timeout. Wolverine assigns the exclusive listeners through its agent/leadership machinery, so the
    /// split is only observable once that settles -- and a fixed sleep either wastes time or races it.
    /// </summary>
    /// <remarks>
    /// "Usable" does not mean every slot is claimed -- an unclaimed slot is simply never chosen as a
    /// target (see <see cref="findAccountIdOwnedBy"/>), and requiring all of them made the suite hostage
    /// to the slowest listener to come up. What the tests need is that no slot is contested and both
    /// nodes own something. The observation is confirmed against the previous one so a mid-handoff
    /// snapshot is not mistaken for a settled one.
    /// </remarks>
    private async Task<Dictionary<int, IHost>> waitForStablePartitionOwnershipAsync()
    {
        var deadline = DateTimeOffset.UtcNow.AddSeconds(120);
        Dictionary<int, IHost>? confirming = null;

        while (DateTimeOffset.UtcNow < deadline)
        {
            var owners = new Dictionary<int, IHost>();
            var contested = false;

            for (var slot = 0; slot < SlotCount; slot++)
            {
                var claimants = new[] { _nodeA, _nodeB }.Where(host => ownsSlot(host, slot)).ToArray();

                if (claimants.Length == 1)
                {
                    owners[slot] = claimants[0];
                }
                else if (claimants.Length > 1)
                {
                    // Mid-handoff: two nodes briefly report Accepting for one slot
                    contested = true;
                }
            }

            var usable = !contested && owners.Values.Contains(_nodeA) && owners.Values.Contains(_nodeB);

            if (usable && confirming != null && sameOwnership(confirming, owners))
            {
                return owners;
            }

            confirming = usable ? owners : null;
            await Task.Delay(500.Milliseconds());
        }

        throw new TimeoutException(
            $"Partition ownership across the two nodes did not settle within 120 seconds. Current state: {describeOwnership()}");
    }

    private static bool sameOwnership(Dictionary<int, IHost> left, Dictionary<int, IHost> right)
    {
        return left.Count == right.Count
               && left.All(pair => right.TryGetValue(pair.Key, out var host) && ReferenceEquals(host, pair.Value));
    }

    private static bool ownsSlot(IHost host, int slot)
    {
        var runtime = host.Services.GetRequiredService<IWolverineRuntime>();
        var uri = slotUri(host, slot);
        return runtime.Endpoints.FindListeningAgent(uri) is { Status: ListeningStatus.Accepting };
    }

    private static Uri slotUri(IHost host, int slot)
    {
        var runtime = host.Services.GetRequiredService<IWolverineRuntime>();
        var topology = runtime.Options.MessagePartitioning.GlobalPartitionedTopologies.Single();
        return topology.ExternalTopology!.Slots[slot].Uri;
    }

    private string describeOwnership()
    {
        return Enumerable.Range(0, SlotCount)
            .Select(slot =>
            {
                var a = ownsSlot(_nodeA, slot) ? "A" : "";
                var b = ownsSlot(_nodeB, slot) ? "B" : "";
                return $"slot {slot}: [{a}{b}]";
            })
            .Join(", ");
    }

    /// <summary>
    /// A different account id resolving to the same sequential lane as <paramref name="seed"/>.
    /// </summary>
    private string findSameLaneCompanion(string seed)
    {
        var target = laneOf(seed);
        return candidateAccountIds().First(id => id != seed && laneOf(id) == target);
    }

    /// <summary>
    /// An account id on the same slot as <paramref name="seed"/> but a different lane within it. The slot
    /// has to match, not merely the owning node: a different slot is a different companion queue.
    /// </summary>
    private string findDifferentLaneOnTheSameSlot(string seed)
    {
        var target = laneOf(seed);
        return candidateAccountIds().First(id =>
        {
            var lane = laneOf(id);
            return lane.Slot == target.Slot && lane.Lane != target.Lane;
        });
    }

    private (int Slot, int Lane) laneOf(string accountId)
    {
        var rules = _nodeA.Services.GetRequiredService<IWolverineRuntime>().Options.MessagePartitioning;
        var slot = new Envelope(new PostEntry(accountId, 0)).SlotForSending(SlotCount, rules);

        // The lane count that matters is the COMPANION queue's, because that is where execution happens --
        // the external slot listener is bridged into it.
        var topology = _nodeA.Services.GetRequiredService<IWolverineRuntime>()
            .Options.MessagePartitioning.GlobalPartitionedTopologies.Single();
        var lanes = (int)topology.LocalTopology!.Slots[slot].GroupShardingSlotNumber!.Value;

        var lane = new Envelope(new PostEntry(accountId, 0)).SlotForProcessing(lanes, rules);
        return (slot, lane);
    }

    private static IEnumerable<string> candidateAccountIds() =>
        Enumerable.Range(0, 2000).Select(i => $"account-{i:D4}");

    /// <summary>
    /// Find an account id whose slot -- resolved by exactly the hash the routing layer uses -- belongs to
    /// the requested node.
    /// </summary>
    private string findAccountIdOwnedBy(IHost owner, Dictionary<int, IHost> owners)
    {
        var rules = _nodeA.Services.GetRequiredService<IWolverineRuntime>().Options.MessagePartitioning;

        foreach (var candidate in candidateAccountIds())
        {
            var slot = new Envelope(new PostEntry(candidate, 0)).SlotForSending(SlotCount, rules);
            if (owners.TryGetValue(slot, out var host) && ReferenceEquals(host, owner))
            {
                return candidate;
            }
        }

        throw new InvalidOperationException(
            $"No candidate account id hashed to a slot owned by the requested node. Ownership: {describeOwnership()}");
    }
}

public record NodeName(string Value);

/// <param name="NestedAccountId">
/// When set, the handler makes a nested awaited invocation for this account id from inside its own
/// execution -- the re-entrancy shape the lane guard exists to refuse.
/// </param>
public record PostEntry(string AccountId, int Amount, string? NestedAccountId = null);

public record NestedEntry(string AccountId);

public record NestedEntryPosted(string AccountId, string HandledBy);

public record PostBadEntry(string AccountId);

public record EntryPosted(string AccountId, int Amount, string HandledBy);

public static class LedgerHandler
{
    private static int _inFlight;

    public static int MaxConcurrency;
    public static readonly ConcurrentBag<string> Handled = new();

    public static string? NestedOutcome;
    public static string? NestedMessage;
    public static TimeSpan NestedElapsed;

    public static void Reset()
    {
        _inFlight = 0;
        MaxConcurrency = 0;
        Handled.Clear();
        NestedOutcome = null;
        NestedMessage = null;
        NestedElapsed = TimeSpan.Zero;
    }

    public static async Task<EntryPosted> Handle(PostEntry command, NodeName node, IMessageContext bus)
    {
        var observed = Interlocked.Increment(ref _inFlight);
        trackConcurrency(observed);
        try
        {
            Handled.Add($"{node.Value}:{command.AccountId}");

            if (command.NestedAccountId is { } nested)
            {
                await invokeNestedAsync(nested, bus);
            }

            await Task.Delay(150);
            return new EntryPosted(command.AccountId, command.Amount, node.Value);
        }
        finally
        {
            Interlocked.Decrement(ref _inFlight);
        }
    }

    public static NestedEntryPosted Handle(NestedEntry command, NodeName node) =>
        new(command.AccountId, node.Value);

    /// <summary>
    /// What a nested awaited invocation did from inside a handler. The elapsed time is what separates an
    /// outright refusal from a deadlock that resolved by timing out -- both throw.
    /// </summary>
    private static async Task invokeNestedAsync(string nestedAccountId, IMessageContext bus)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            await bus.InvokeAsync<NestedEntryPosted>(new NestedEntry(nestedAccountId),
                new DeliveryOptions { InvokeThroughRouting = true }, CancellationToken.None, 30.Seconds());
            NestedOutcome = "succeeded";
        }
        catch (Exception e)
        {
            NestedOutcome = e.GetType().Name;
            NestedMessage = e.Message;
        }
        finally
        {
            NestedElapsed = stopwatch.Elapsed;
        }
    }

    public static EntryPosted Handle(PostBadEntry command) =>
        throw new InvalidOperationException("this entry cannot be posted");

    private static void trackConcurrency(int observed)
    {
        int current;
        while ((current = Volatile.Read(ref MaxConcurrency)) < observed)
        {
            if (Interlocked.CompareExchange(ref MaxConcurrency, observed, current) == current) return;
        }
    }
}
