namespace go order

struct Order {
    1: string order_id
    2: string user_id
    3: i64    amount_cents
    4: string status        // PENDING | PAID | FAILED
    5: string created_at    // RFC 3339
}

// code mirrors HTTP semantics so the gateway can map it directly: 400, 404, 409, 422
exception OrderError {
    1: i32    code
    2: string message
}

struct CreateOrderRequest {
    1: required string user_id
    2: required i64    amount_cents
    3: required string idempotency_key
}
struct CreateOrderResponse {
    1: Order order
    2: bool  replayed       // true if this key was seen before and the original order is returned
}

struct GetOrderRequest {
    1: required string order_id
}
struct GetOrderResponse {
    1: Order order
}

struct UpdateStatusRequest {
    1: required string order_id
    2: required string status  // PAID | FAILED
}
struct UpdateStatusResponse {
    1: Order order
}

service OrderService {
    CreateOrderResponse  CreateOrder(1: CreateOrderRequest req)   throws (1: OrderError err)
    GetOrderResponse     GetOrder(1: GetOrderRequest req)         throws (1: OrderError err)
    UpdateStatusResponse UpdateStatus(1: UpdateStatusRequest req) throws (1: OrderError err)
}
