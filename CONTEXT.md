# Eve Raft

Eve Raft connects a Raft workspace to an Eve agent while keeping application-specific identity and authorization outside the integration.

## Language

**Consumer**:
The application or team that installs and operates Eve Raft for its own Eve agent.
_Avoid_: Customer, client, host

**Raft channel**:
The Eve channel through which Raft conversations, tasks, attachments, activity, and human input are exchanged with an Eve agent.
_Avoid_: Bridge, bot

**Raft principal**:
A stable actor identity derived from Raft and used by default when invoking the Eve agent.
_Avoid_: Dex user, linked account

**Principal resolver**:
An optional consumer-owned mapping from a Raft principal to the consumer's application identity.
_Avoid_: Account linking

**Direct conversation**:
A one-to-one Raft conversation where every eligible message invokes the Eve agent.
_Avoid_: DM channel, private channel

**Shared conversation**:
A multi-participant Raft conversation where an explicit mention or a continued agent thread invokes the Eve agent.
_Avoid_: Group DM, room

**Raft task**:
Work assigned to the Eve agent in Raft and advanced through `todo`, `in_progress`, and `in_review`, but never completed automatically.
_Avoid_: Job, issue

**Activity indicator**:
A Raft-visible lifecycle update that communicates agent progress without exposing private agent content.
_Avoid_: Chain of thought, reasoning trace

**Human-input prompt**:
A Markdown prompt with numbered choices through which the Eve agent pauses for a person's decision.
_Avoid_: Rich card, form

**Persistent state**:
Durable operator-owned storage for Raft credentials and delivery state that survives process and host restarts.
_Avoid_: Environment configuration
