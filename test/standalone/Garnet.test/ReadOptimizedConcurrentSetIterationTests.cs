// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
using Garnet.common;
using NUnit.Framework;

namespace Garnet.test
{
    [TestFixture]
    public class ReadOptimizedConcurrentSetIterationTests
    {
        [Test]
        public async Task RemovalBetweenIteratorStepsSkipsTheRemainingItem()
        {
            var items = new ReadOptimizedConcurrentSet<int>();
            items.TryAdd(1);
            items.TryAdd(2);

            var index = 0;
            Assert.That(items.Iterate(ref index, out var first), Is.True);
            Assert.That(first, Is.EqualTo(1));
            Assert.That(index, Is.EqualTo(1));

            // Iterate has released its read lock. A different thread can acquire the write
            // lock and remove the returned item before the next iterator step.
            var removed = await Task.Run(() => items.TryRemove(first)).WaitAsync(TimeSpan.FromSeconds(5));
            Assert.That(removed, Is.True);
            Assert.That(items.Count, Is.EqualTo(1));

            var hasNext = items.Iterate(ref index, out _);
            TestContext.WriteLine($"After removing {first}: Count={items.Count}, index={index}, hasNext={hasNext}");
            Assert.That(hasNext, Is.False, "Reproduces the current iterator skipping item 2 after it shifts to index 0.");

            var freshIndex = 0;
            Assert.That(items.Iterate(ref freshIndex, out var remaining), Is.True);
            Assert.That(remaining, Is.EqualTo(2), "The skipped item is still present in the collection.");
        }
    }
}