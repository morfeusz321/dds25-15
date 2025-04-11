# Distributed Data Systems project

This document goes over our implemented features and design choices we made to make it easier for the grading team to evalute our system and understand the code.

## Event driven design

We designed the transactions to be event-driven as opposed to request-response architecture. This means that when a user starts a checkout it sends event messages to the other services but returns an immeadite 200 OK response that the order is processing. This doesn't necessarily mean the order is succesful only that it started the checkout process. When the order is successful the order status in the databse will be set to paid. The user can check this status to learn if the order checkout went through or not.

**Note:** this also means the default consistency check will not correctly show the numbers for the log inconsistencies as that is checking the exact responses, and in our case that doesn't indicate whether an order was successful or not.

## SAGA and eventual consistency

Our system uses the SAGA pattern, with the order service acting as the orchestrator. This makes our system eventually consistent. We employ rolling back of the stock and payment in case the event handling on the other service does not go through. This makes it so that there might be less stock or money at a given moment than there should be but given enough time the rollbacks will eventually catch up and the numbers in the databse will be consistent.

**Note:** The given consistency check also needs to be adjusted because of this since that checks inconsistencies in the database immediately after the last order checkout but in our case our system needs extra time to be consistent after the last order checkout because of the eventual consistency.

## Kafka message brokers

We use kafka to send and receive event messages between the microservices. This makes it so that the events don't have to be processed in the exact order they were sent, but more importantly it enables a level of fault tolerance. If a service is down while another service sends some messages to it, kafka makes sure that these events still arrive at the faulty service once it comes back online.

## Database fault tolerance

## Service fault tolerance
