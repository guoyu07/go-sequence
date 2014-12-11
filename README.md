# GO Sequence
A sequence generator implemented by golang

## How?
 - MySQL for persist storage to record app, bucket and cursor
 - Redis for atomic POP

## 2 types of seqs will be supported

 - id seq: a pure numeric seq, you just request, I would give you next increment number.
 - id-word seq: a id-word map seq, you request with a word, If that word already exists, I would give you mapped id for that word. Or else, I would make a new id-word mapper for you.
