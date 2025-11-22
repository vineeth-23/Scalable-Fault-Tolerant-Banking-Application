package node

func (n *Node) tryLockIntraShard(sender, receiver string) bool {
	if n.LockTable[sender] || n.LockTable[receiver] {
		return false
	}
	n.LockTable[sender] = true
	n.LockTable[receiver] = true
	return true
}

func (n *Node) unlockIntraShard(sender, receiver string) {
	delete(n.LockTable, sender)
	delete(n.LockTable, receiver)
}
