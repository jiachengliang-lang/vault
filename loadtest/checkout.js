// k6 load test: checkouts at a FIXED arrival rate (RATE per second, for DURATION), from many users.
//
// Fixed arrival rate matters. A closed loop ("N users, each waits for its response, then sends
// again") sends less traffic exactly when the server slows down, so it under-reports latency
// (coordinated omission). Real users arrive independently of how slow we are; so does this test.
//
//   RATE=400 DURATION=30s k6 run loadtest/checkout.js
import http from 'k6/http';
import { check } from 'k6';
import { SharedArray } from 'k6/data';

const tokens = new SharedArray('tokens', () => JSON.parse(open('./tokens.json')));

export const options = {
  scenarios: {
    checkout: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 300),
      timeUnit: '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: 200,
      maxVUs: 3000,
    },
  },
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
  thresholds: { http_req_duration: ['p(99)<200'] }, // the checkout SLO
};

export default function () {
  // Random user per request: 500 users at 1,000 req/s is ~2 req/s each, under the per-user rate limit.
  const token = tokens[Math.floor(Math.random() * tokens.length)];
  const res = http.post('http://localhost:8080/v1/checkout',
    JSON.stringify({ amount_cents: 1999 }),
    { headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${token}`,
      'Idempotency-Key': `${__VU}-${__ITER}-${Date.now()}`,
    } });
  check(res, { 'status 201': (r) => r.status === 201 });
}
