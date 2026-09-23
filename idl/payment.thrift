namespace go payment

struct Payment {
    1: string payment_id
    2: string order_id
    3: i64    amount_cents
    4: string status        // PENDING | SUCCEEDED | DECLINED
}

exception PaymentError {
    1: i32    code
    2: string message
}

struct ChargeRequest {
    1: required string order_id
    2: required i64    amount_cents
    3: required string idempotency_key
}
struct ChargeResponse {
    1: Payment payment
    2: bool    replayed
}

service PaymentService {
    ChargeResponse Charge(1: ChargeRequest req) throws (1: PaymentError err)
}
