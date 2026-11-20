---
title: "BeamFoundry Rays Data Mongo"
type: docs
---

# BeamFoundry Rays Data Mongo

**Rays Data Mongo** is the official MongoDB integration package for the **BeamFoundry Rays** framework,
bringing Spring Data MongoDB-style auto-generated repositories, a fluent client builder, and ThreadLocal
transactions to Go using the official [MongoDB Go Driver](https://www.mongodb.com/docs/drivers/go/current/).

By bridging Rays' Inversion of Control (IoC) container with MongoDB and ThreadLocal session storage,
this package eliminates repetitive BSON filter boilerplate and explicit `mongo.SessionContext` passing
across service layers while letting you register one or more MongoDB clients via standard `Configuration` Beams.

### Comprehensive Guide
1. [Introduction & Architecture](/rays-data-mongo/docs/1-introduction/)
2. [Quickstart & Setup](/rays-data-mongo/docs/2-quickstart/)
3. [Client Builder & Multi-Database](/rays-data-mongo/docs/3-configuration/)
4. [Auto-Generated Repositories](/rays-data-mongo/docs/4-repositories/)
5. [ThreadLocal Transactions](/rays-data-mongo/docs/5-transactions/)
6. [Logging Integration](/rays-data-mongo/docs/6-logging/)
7. [Testing & Environments](/rays-data-mongo/docs/7-testing-environments/)
8. [Advanced Querying](/rays-data-mongo/docs/8-advanced-querying/)
