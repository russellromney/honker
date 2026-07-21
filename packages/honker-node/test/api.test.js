'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const buildApi = require('../api');

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
