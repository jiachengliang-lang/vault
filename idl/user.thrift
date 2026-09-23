namespace go user

struct Profile {
    1: string user_id
    2: string email
    3: string address
}

exception UserError {
    1: i32    code
    2: string message
}

// Every call that touches PII says who is asking and why. The audit log records both.
struct Accessor {
    1: required string actor   // e.g. "user:<id>" or "support:<id>"
    2: required string reason
}

struct UpsertProfileRequest {
    1: required string   user_id
    2: required string   email
    3: optional string   address
    4: required Accessor accessor
}
struct UpsertProfileResponse {
    1: Profile profile
}

struct GetProfileRequest {
    1: required string   user_id
    2: required Accessor accessor
}
struct GetProfileResponse {
    1: Profile profile
}

struct DeleteUserRequest {
    1: required string   user_id
    2: required Accessor accessor
}
struct DeleteUserResponse {}

service UserService {
    UpsertProfileResponse UpsertProfile(1: UpsertProfileRequest req) throws (1: UserError err)
    GetProfileResponse    GetProfile(1: GetProfileRequest req)       throws (1: UserError err)
    DeleteUserResponse    DeleteUser(1: DeleteUserRequest req)       throws (1: UserError err)
}
