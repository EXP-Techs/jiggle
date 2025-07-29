# TRPC: The Unified RPC & Messaging Protocol for Go

TRPC is a high-performance, type-safe communication protocol for microservices that elegantly unifies synchronous RPC (Request/Response) and asynchronous eventing (Pub/Sub) into a single, topic-based framework. It solves the complexity of managing separate systems for direct calls and event-driven messaging by providing one simple, contract-driven workflow.

## Core Features

* **Unified Communication:** Use a single client and protocol for both synchronous `Request()` calls and asynchronous `Publish()` events.
* **Type-Safe by Design:** Define your service contracts in simple `.trpc` files. A powerful code generator creates type-safe Go clients and server interfaces, eliminating entire classes of runtime errors.
* **Decoupled & Scalable:** Services are completely decoupled, communicating only through topics managed by a central broker. This allows for independent scaling and development.
* **Production-Ready Broker:** The included broker is built for reliability, featuring:
    * **Load Balancing:** Automatically distributes requests across multiple instances of a service.
    * **Persistence:** Uses a Write-Ahead Log (WAL) and Redis to ensure messages aren't lost.
    * **Guaranteed Delivery:** Supports ACK/NACK and configurable retries with exponential backoff.
    * **Dead-Letter Queues (DLQ):** Failed messages are automatically routed to a DLQ topic for inspection.
* **Real-time Management GUI:** A built-in web dashboard provides real-time monitoring and configuration of topics, clients, and broker settings.
* **Secure by Default:** Supports TLS for encrypted communication and authenticates clients via a configurable user store in Redis.
* **Observable:** Exposes key operational data via a `/metrics` endpoint for Prometheus monitoring.

## Project Structure

The TRPC ecosystem consists of three main components:

1.  **`trpc-broker`:** The central server that handles client connections, authentication, and message routing.
2.  **`trpc-gen-go`:** The command-line tool that parses `.trpc` contract files and generates type-safe Go code.
3.  **`driver`:** The core Go client library that the generated code imports. It manages the low-level connection, authentication, and communication with the broker.

## Getting Started

### Prerequisites

* Go 1.21+
* Redis (for configuration and message storage)
* `openssl` (for generating test TLS certificates)

### 1. Run the Broker

First, start the TRPC Broker. It will connect to Redis and begin listening for client connections.

```bash
# Navigate to the broker's directory
# (This is the file with id: trpc_broker_latest_version)
go run main.go
