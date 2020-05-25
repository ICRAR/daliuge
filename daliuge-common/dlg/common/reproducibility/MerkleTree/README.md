# Merkle Tree
An implementation of a MerkleTree in Python

## MerkleTrees

A Merkle-Tree is a binary tree constructed from a list of data elements (the leaves of the tree). Each leaf is labelled 
with a cryptographic hash of its contents with all internal nodes labelled with the cryptographic hash of the 
cryptographic hashes of its children.

Doing so guarantees that if two nodes' hashes match they represent the same Merkle Tree (since this is a recursively 
defined data-structure).

Thus one can compare a vast amount of content by comparing only root-hashes. 

Our implementation makes the following restriction:

  - The initial data must be in a Python List.  
  
## Installation on Windows
Installing the Cryptography library can be a pain on windows platforms
[help](https://stackoverflow.com/questions/45089805/pip-install-cryptography-in-windows)