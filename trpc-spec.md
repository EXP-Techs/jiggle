# The .trpc File Specification (v1.0)

## 1. Introduction

The `.trpc` file is the heart of the **Topic-based Remote Procedure Call** protocol. It serves as a single, authoritative source of truth for defining the communication contract between your services. By defining all your data structures, events, and RPC calls in one place, you enable TRPC's powerful code generation and ensure type-safe, reliable communication across your entire architecture.

The syntax is heavily inspired by Protocol Buffers (`.proto`) for its clarity, simplicity, and widespread familiarity.

## 2. File Structure and Syntax

A `.trpc` file is a plain text file that defines three primary top-level concepts:

* `message`: Defines the structure of the data you send and receive.
* `service`: Groups related communication methods (`rpc` and `event`) together.
* `import`: Allows you to import definitions from other `.trpc` files.

```proto
// Use C-style comments for documentation.

// Define the version of the TRPC syntax.
syntax = "trpc/v1";

// Define the package, similar to a namespace.
package user.v1;

// Import definitions from other files for better organization.
import "common/v1/common.trpc";

// --- Message Definitions ---
message User {
    string user_id = 1;
    string name = 2;
    string email = 3;
    common.v1.Timestamp created_at = 4;
}

// --- Service Definitions ---
service UserService {
    // RPC (Synchronous) and Event (Asynchronous) definitions go here.
}
````

## 3\. Defining Messages

A `message` defines a structured data type. It consists of a series of typed fields, each with a unique field number. These field numbers are crucial for binary serialization and ensuring backward compatibility.

```proto
message GetUserRequest {
  // Field type: string
  // Field name: user_id
  // Field number: 1
  string user_id = 1;
}

message UserProfileResponse {
  User user = 1;
  bool is_active = 2;
}
```

### Scalar Value Types

| .trpc Type | Notes | Corresponding Type (Go) |
| :--- | :--- | :--- |
| `float64` | 64-bit floating-point | `float64` |
| `float32` | 32-bit floating-point | `float32` |
| `int32` | 32-bit integer | `int32` |
| `int64` | 64-bit integer | `int64` |
| `uint32` | Unsigned 32-bit integer | `uint32` |
| `uint64` | Unsigned 64-bit integer | `uint64` |
| `bool` | Boolean true or false | `bool` |
| `string` | UTF-8 encoded string | `string` |
| `bytes` | Raw sequence of bytes | `[]byte` |

## 4\. Defining Services

A `service` is a logical grouping of communication methods related to a specific domain (e.g., `UserService`, `PaymentService`). A service can contain both `rpc` and `event` definitions.

```proto
service UserService {
  // ... methods go here
}
```

### `rpc`: Synchronous Request-Response

An `rpc` defines a synchronous interaction. It's a classic remote procedure call where a client sends a request and waits for a response. It is defined by an input message and an output message.

**Syntax:** `rpc MethodName(RequestMessage) returns (ResponseMessage);`

```proto
service UserService {
  // This defines a topic named "user.v1.UserService.GetUserProfile".
  // A client can make a request to this topic with a GetUserRequest message
  // and will receive a UserProfileResponse in return.
  rpc GetUserProfile(GetUserRequest) returns (UserProfileResponse);
}
```

### `event`: Asynchronous Notification

An `event` defines an asynchronous, "fire-and-forget" interaction. A client publishes a message to the topic and does not wait for a response. It is defined by a single message payload.

**Syntax:** `event EventName(PayloadMessage);`

```proto
service UserService {
  // This defines a topic named "user.v1.UserService.UserCreated".
  // A client can publish a User message to this topic to notify
  // other services that a new user has been created.
  event UserCreated(User);
}
```

## 5\. Putting It All Together: A Complete Example

Here is a complete example of a `.trpc` file that defines a simple `UserService`.

`user/v1/user_service.trpc`

```proto
syntax = "trpc/v1";

package user.v1;

import "google/protobuf/timestamp.proto"; // Standard proto files can be used

// ========== MESSAGE DEFINITIONS ==========

message User {
  string user_id = 1;
  string name = 2;
  string email = 3;
  google.protobuf.Timestamp created_at = 4;
}

message GetUserRequest {
  string user_id = 1;
}

message CreateUserRequest {
  string name = 1;
  string email = 2;
  string password = 3;
}


// ========== SERVICE DEFINITION ==========

// The UserService handles all operations related to user accounts.
service UserService {

  // A synchronous call to retrieve a user's profile by their ID.
  // This is a typical "read" operation.
  rpc GetUserProfile(GetUserRequest) returns (User);

  // A synchronous call to create a new user.
  // Returns the newly created user object, including its generated ID.
  rpc CreateUser(CreateUserRequest) returns (User);

  // An asynchronous event that is published whenever a user's
  // profile information is updated. Other services (e.g., search, analytics)
  // can subscribe to this topic to stay in sync.
  event UserUpdated(User);

  // An asynchronous event that is published when a user is deleted.
  event UserDeleted(GetUserRequest); // Can reuse request messages
}
```

This single file provides everything the `trpc-gen` tool needs to create a fully-typed, easy-to-use communication layer for the `UserService`.
