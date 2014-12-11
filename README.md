# GO Sequence
A sequence generator implemented by golang

## How?
 - MySQL for persist storage to record app, bucket and cursor
 - Redis for atomic POP

## 2 types of seqs will be supported

 - id seq, a pure numric seq
 - id-word seq, a id-word map seq
