'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const buildApi = require('../api');

// The unit under test in this file is waitForUpdateOrTimeout in
// ../api.js — the shared wait helper used by ClaimWaker, Listener, and
// Scheduler. The ClaimWaker is just the most convenient public handle.

test('claim waker reuses a pending update wait across poll timeouts', async () => {
  const { ClaimWaker } = buildApi({});
  let nextCalls = 0;
  let closed = false;
  const updateEvents = {
    next() {
      nextCalls += 1;
      return new Promise(() => {});
    },
    close() {
      closed = true;
    },
  };
  const queue = {
    _db: { updateEvents: () => updateEvents },
    claimOne: () => null,
    _nextClaimAt: () => null,
  };
  const controller = new AbortController();
  const abortTimer = setTimeout(() => controller.abort(), 50);
  const waker = new ClaimWaker(queue, { idlePollS: 0.001 });

  try {
    await waker.next('worker', { signal: controller.signal });
    assert.equal(
      nextCalls,
      1,
      'poll timeouts must not start additional native update waits'
    );
  } finally {
    clearTimeout(abortTimer);
    waker.close();
  }

  assert.equal(closed, true);
});

test('poll timeouts do not accumulate waiters on the shared update wait', async () => {
  const { ClaimWaker } = buildApi({});
  let resolveNative;
  const updateEvents = {
    next() {
      return new Promise((resolve) => {
        resolveNative = resolve;
      });
    },
    close() {},
  };
  const queue = {
    _db: { updateEvents: () => updateEvents },
    claimOne: () => null,
    _nextClaimAt: () => null,
  };
  const controller = new AbortController();
  const abortTimer = setTimeout(() => controller.abort(), 50);
  const waker = new ClaimWaker(queue, { idlePollS: 0.001 });

  try {
    await waker.next('worker', { signal: controller.signal });
  } finally {
    clearTimeout(abortTimer);
    waker.close();
  }

  // Each timed-out race must detach from the shared wait. Racing the
  // cached promise directly would retain one settled-race closure per
  // poll until the next commit — unbounded growth on an idle database.
  const record = buildApi._pendingUpdateWaits.get(updateEvents);
  assert.ok(record, 'the native wait is still pending after the abort');
  assert.equal(record.waiters.size, 0, 'timed-out polls must unsubscribe');

  // The pending native wait still observes the next update, then evicts.
  resolveNative();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(buildApi._pendingUpdateWaits.get(updateEvents), undefined);
});
