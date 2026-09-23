// k6 load test: ramps up checkout traffic from many distinct users.
// Tokens come from `make load` (tokengen -n), so the per-user rate limiter behaves like real traffic.
// Record p50/p95/p99 BEFORE and AFTER each optimization.
import http from 'k6/http';
import { check, sleep } from 'k6';
import { SharedArray } from 'k6/data';

const tokens = new SharedArray('tokens', () => JSON.parse(open('./tokens.json')));

export const options = {
  stages: [
    { duration: __ENV.RAMP || '30s', target: 50 },
    { duration: __ENV.HOLD || '1m', target: 200 },
    { duration: __ENV.RAMP || '30s', target: 0 },
  ],
  thresholds: { http_req_duration: ['p(99)<200'] }, // your SLO; adjust once you have a baseline
};

export default function () {
  const token = tokens[(__VU - 1) % tokens.length];
  const res = http.post('http://localhost:8080/v1/checkout',
    JSON.stringify({ amount_cents: 1999 }),
    { headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${token}`,
      'Idempotency-Key': `${__VU}-${__ITER}-${Date.now()}`,
    } });
  check(res, { 'status 201': (r) => r.status === 201 });
  sleep(0.2); // think time: ~5 req/s per user, under the 20 req/s per-user limit
}
