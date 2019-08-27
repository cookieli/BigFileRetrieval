package lzx.retrieval.storage;

import lzx.retrieval.KVPair;

public abstract class BTreeNode {
	public static final int DEFAULT_Node_SIZE = 3;
	public static final int MINIMAL_B_ORDER = 3;
	public static final String nodeSuffix = ".nd";
	public static int NodeCnt = 0;
	public int curNodeSize;
	public int NodeNumber;
	public String nodeFile = null;
	public boolean isLeaf;
	public boolean isHeader;
	public boolean isInternal;
	public int father;
	public int getFather() {
		return father;
	}
	public void setNodeFile() {
		nodeFile = "" + NodeNumber + nodeSuffix;
	}
	
	public void setNodeNumber() {
		this.NodeNumber = assignNodeNumber();
		setNodeFile();
	}
	public void setFather(int father) {
		this.father = father;
	}
	
	public static int assignNodeNumber() {
		return NodeCnt++;
	}
	public int rightFatherEntry;//it is father's larger than entry
	
	public boolean isLeaf() {
		return isLeaf;
	}
	public void setLeaf(boolean isLeaf) {
		this.isLeaf = isLeaf;
	}
	public boolean isHeader() {
		return isHeader;
	}
	public void setHeader(boolean isHeader) {
		this.isHeader = isHeader;
		this.NodeNumber = 0;
		setNodeFile();
		
		this.father = -1;
		
	}
	public boolean isInternal() {
		return isInternal;
	}
	public void setInternal(boolean isInternal) {
		this.isInternal = isInternal;
	}
	
	public abstract KVPair search(byte[] key, String fileName);
	
	public abstract int entryNum();
	public abstract BTreeNode[] split();
	public abstract void merge(BTreeNode NodeA, BTreeNode NodeB);
	public abstract String toString(String fileName);
	public abstract void flushDataToFile(String srcFile);
	public int getNodeNumber() {
		return NodeNumber;
	}
	public void setNodeNumber(int NodeNumber) {
		this.NodeNumber = NodeNumber;
	}
	public abstract byte[] toByteArray();
}
