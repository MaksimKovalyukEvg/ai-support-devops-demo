# Architecture

## Runtime flow

Client
  -> Backend API
  -> Relay service
  -> AI provider
  -> Backend API
  -> Client

The backend validates incoming requests, prepares the prompt, anonymizes sensitive fields and adds retrieval context when available.

## Knowledge pipeline

The knowledge pipeline converts historical support dialogs into structured knowledge cards.

Stages:

1. Input normalization.
2. Message extraction.
3. QA pair extraction.
4. Clustering.
5. Card cleanup.
6. Quality rating.
7. Retrieval pack generation.

## Orchestrator

The orchestrator runs pipeline stages sequentially, stores progress, shows logs and supports resume after interruption.
