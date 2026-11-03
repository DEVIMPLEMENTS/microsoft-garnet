// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using Tsavorite.core;

namespace Garnet.test
{
    [TestFixture]
    public class SubscribeBrokerTests
    {
        LightEpoch epoch;
        SubscribeBroker broker;

        [SetUp]
        public void SetUp()
        {
            epoch = new LightEpoch();
            broker = new SubscribeBroker(null, 1 << 20, epoch);
        }

        [TearDown]
        public void TearDown()
        {
            broker.Dispose();
            epoch.Dispose();
        }

        [TestCase(false)]
        [TestCase(true)]
        public async Task UnsubscribingDuringBroadcastDoesNotSkipTheNextSubscriber(bool pattern)
        {
            using var entered = new ManualResetEventSlim();
            using var resume = new ManualResetEventSlim();
            var channel = "notifications"u8.ToArray();
            var subscription = pattern ? "notification*"u8.ToArray() : channel;
            var firstCalls = 0;
            var secondCalls = 0;
            using var first = new CallbackSession(() =>
            {
                Interlocked.Increment(ref firstCalls);
                entered.Set();
                if (!resume.Wait(TimeSpan.FromSeconds(10)))
                    throw new TimeoutException("The broadcast was not resumed.");
            });
            using var second = new CallbackSession(() => Interlocked.Increment(ref secondCalls));
            Subscribe(broker, subscription, first, pattern);
            Subscribe(broker, subscription, second, pattern);

            var publishing = Task.Run(() => Publish(broker, channel));
            try
            {
                Assert.That(entered.Wait(TimeSpan.FromSeconds(10)), Is.True);
                Assert.That(Unsubscribe(broker, subscription, first, pattern), Is.True);
            }
            finally
            {
                resume.Set();
                await publishing.WaitAsync(TimeSpan.FromSeconds(10));
            }

            Assert.That(firstCalls, Is.EqualTo(1));
            Assert.That(secondCalls, Is.EqualTo(1), "Removing the first subscriber must not skip the second.");
            Assert.That(await publishing, Is.EqualTo(2));
            Publish(broker, channel);
            Assert.That(firstCalls, Is.EqualTo(1), "The removed subscriber must not receive a later broadcast.");
            Assert.That(secondCalls, Is.EqualTo(2));
        }

        [TestCase(false)]
        [TestCase(true)]
        public void CallbackCanUnsubscribeItselfWithoutSkippingTheNextSubscriber(bool pattern)
        {
            var channel = "notifications"u8.ToArray();
            var subscription = pattern ? "notification*"u8.ToArray() : channel;
            var secondCalls = 0;
            CallbackSession first = null;
            using (first = new CallbackSession(() => Unsubscribe(broker, subscription, first, pattern)))
            using (var second = new CallbackSession(() => secondCalls++))
            {
                Subscribe(broker, subscription, first, pattern);
                Subscribe(broker, subscription, second, pattern);
                Assert.That(Publish(broker, channel), Is.EqualTo(2));
                Assert.That(secondCalls, Is.EqualTo(1));
            }
        }

        [Test]
        public void SnapshotsRemainStableAndReflectSubsequentChanges()
        {
            var items = new ReadOptimizedConcurrentSet<int>();
            Assert.That(items.GetSnapshot().IsEmpty, Is.True);
            items.TryAdd(1);
            var first = items.GetSnapshot();
            items.TryAdd(2, out _);
            var second = items.GetSnapshot();
            items.TryAddAndGet(3, out _);
            Assert.That(items.GetSnapshot().ToArray(), Is.EqualTo(new[] { 1, 2, 3 }));
            items.TryRemove(1);
            Assert.That(items.GetSnapshot().ToArray(), Is.EqualTo(new[] { 2, 3 }));
            items.Clear();
            Assert.That(items.GetSnapshot().IsEmpty, Is.True);
            Assert.That(first.ToArray(), Is.EqualTo(new[] { 1 }));
            Assert.That(second.ToArray(), Is.EqualTo(new[] { 1, 2 }));
        }

        static unsafe void Subscribe(SubscribeBroker broker, byte[] channel, ServerSessionBase session, bool pattern)
        {
            fixed (byte* pointer = channel)
            {
                var key = PinnedSpanByte.FromPinnedPointer(pointer, channel.Length);
                if (pattern)
                    broker.PatternSubscribe(key, session);
                else
                    broker.Subscribe(key, session);
            }
        }

        static bool Unsubscribe(SubscribeBroker broker, byte[] channel, ServerSessionBase session, bool pattern)
        {
            var key = ByteArrayWrapper.CopyFrom(channel, false);
            return pattern ? broker.PatternUnsubscribe(key, session) : broker.Unsubscribe(key, session);
        }

        static unsafe int Publish(SubscribeBroker broker, byte[] channel)
        {
            fixed (byte* pointer = channel)
                return broker.PublishNow(PinnedSpanByte.FromPinnedPointer(pointer, channel.Length), default);
        }

        sealed class CallbackSession(Action callback) : ServerSessionBase(null)
        {
            public override unsafe int TryConsumeMessages(byte* buffer, int bytesRead) => throw new NotSupportedException();

            public override void Publish(PinnedSpanByte key, PinnedSpanByte value) => callback();

            public override void PatternPublish(PinnedSpanByte pattern, PinnedSpanByte key, PinnedSpanByte value) => callback();
        }
    }
}