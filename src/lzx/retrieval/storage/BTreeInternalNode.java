package lzx.retrieval.storage;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import lzx.retrieval.BufferedReader;
import lzx.retrieval.ByteUtils;
import lzx.retrieval.KVPair;
import lzx.retrieval.storage.BTreeLeafNode.NodeKeyPos;

public class BTreeInternalNode extends BTreeNode{
	
	
	public ArrayList<Integer> children;
	
	public int rightSiblingNode = -1;
	
	public BTreeInternalNode(String dictionary) {
		this.dictionary = dictionary;
		isInternal = false;
		isLeaf = true;
	}
	
	public BTreeInternalNode(byte[] bytes, long originFilePos, int ch1, int ch2, String dict) {
		positions = new ArrayList<>();
		data = new ArrayList<>();
		children = new ArrayList<>();
		isInternal = true;
		isLeaf = false;
		positions.add(new NodeKeyPos(0, bytes.length, originFilePos));
		this.nodeFile = "tmp.nd";
		setDictionary(dict);
		this.addKey(bytes);
		curNodeSize = bytes.length;
		children.add(ch1);
		children.add(ch2);
		
	}
	
	public BTreeInternalNode(ArrayList<NodeKeyPos> keys, ArrayList<Integer> children, BTreeInternalNode n, String dict) {
		super(keys, n, dict);
		this.children = new ArrayList<>();
		this.children.addAll(children);
		isInternal = true;
		isLeaf = false;
		//setNodeNumber();
	}
	
	public BTreeInternalNode(ArrayList<NodeKeyPos> keys, ArrayList<Integer> children, BTreeInternalNode n, boolean copy, String dict) {
		super(keys, n, true, dict);
		this.children = new ArrayList<>();
		this.children.addAll(children);
		isInternal = true;
		isLeaf = false;
		
	}
	

	@Override
	public BTreeNode[] split() {
		// TODO Auto-generated method stub
		int mid = positions.size()/2;
		byte[] bytes = this.getCorrespondKey(mid);
		NodeKeyPos key = positions.get(mid);
		BTreeInternalNode child1 = new BTreeInternalNode(new ArrayList<NodeKeyPos>(positions.subList(mid+1, positions.size())),new ArrayList<Integer>(children.subList(mid+1, children.size())), this,this.dictionary);
		BTreeInternalNode child2 = new BTreeInternalNode(new ArrayList<NodeKeyPos>(positions.subList(0, mid)),new ArrayList<Integer>(children.subList(0, mid+1)), this, true,this.dictionary);
		BTreeInternalNode father = new BTreeInternalNode(bytes, key.originFilePos, this.NodeNumber, child1.NodeNumber, this.dictionary);
		child1.father = this.father;
		child2.father = this.father;
		father.rightFatherEntry = this.rightFatherEntry;
		if (this.rightFatherEntry == -1) {
			this.rightFatherEntry = 0;
		}
		child2.rightFatherEntry = this.rightFatherEntry;
		child1.rightFatherEntry = this.rightFatherEntry + 1;
		
		
		return new BTreeNode[] {father, child2, child1};
	}

	@Override
	public void merge(BTreeNode parent, BTreeNode child) {
		// TODO Auto-generated method stub
		BTreeInternalNode pa= (BTreeInternalNode) parent;
		BTreeInternalNode chi = (BTreeInternalNode) child;
		byte[] bytes = chi.getCorrespondKey(0);
		if(chi.rightFatherEntry >= pa.positions.size()) {
			pa.positions.add(new NodeKeyPos(curNodeSize, child.positions.get(0).keyLength, child.positions.get(0).originFilePos));
			pa.children.add(chi.children.get(chi.children.size() - 1));
		} else {
			pa.positions.add(chi.rightFatherEntry, new NodeKeyPos(curNodeSize, child.positions.get(0).keyLength, child.positions.get(0).originFilePos));
			pa.children.add(chi.rightFatherEntry+1, chi.children.get(chi.children.size() - 1));
		}
		pa.addKey(bytes);
		chi.deleteFile();
	}

	@Override
	public byte[] toByteArray() {
		// TODO Auto-generated method stub
		byte[] res = super.toByteArray();
		int childrenSize = this.children.size();
		byte[] bytes;
		bytes = ByteUtils.intToBytes(childrenSize);
		for(int j = 0; j < bytes.length; j++) {
			res[cnt++] = bytes[j];
		}
		for(int i = 0; i < childrenSize; i++) {
			bytes = ByteUtils.intToBytes(this.children.get(i));
			for(int j = 0; j < bytes.length; j++) {
				res[cnt++] = bytes[j];
			}
		}
		return res;
	}
	
	
	@Override
	public void restoreFromByteArr(byte[] bytes) {
		super.restoreFromByteArr(bytes);
		int childSize = ByteUtils.bytesToInt(Arrays.copyOfRange(bytes, cnt, cnt+Integer.BYTES));
		cnt+= Integer.BYTES;
		this.children = new ArrayList<>();
		for(int i = 0; i < childSize ; i++) {
			this.children.add(ByteUtils.bytesToInt(Arrays.copyOfRange(bytes, cnt, cnt+Integer.BYTES)));
			cnt+=Integer.BYTES;
		}
		setDictionary(this.dictionary);
	}
	
	

	public String toString()  {
		StringBuilder sb = new StringBuilder();
		sb.append("Internal Node: " + NodeNumber+": " +this.father+"\n");
		for (int i = 0; i < positions.size(); i++) {
			sb.append(i + " " + new String(getCorrespondKey(i)));
			sb.append(" ");
		}
		sb.append("\n");
		for(int i: children) {
			sb.append(i);
			sb.append(" ");
		}
		sb.append("\n");
		return sb.toString();
	}

	@Override
	public int entryNum() {
		// TODO Auto-generated method stub
		return this.positions.size();
	}
	
	
	@Override
	public long search(byte[] key) {
		// TODO Auto-generated method stub
		for (int i = 0; i< positions.size(); i++) {
			byte[] keyInNode = getCorrespondKey(i);
			if (Arrays.equals(keyInNode, key)) {
				return positions.get(i).originFilePos;
			}else if(KVPair.compare(key, keyInNode) < 0) {
				return BufferedReader.loadBTreeNode(children.get(i), this.dictionary).search(key);
			}
		}
		return BufferedReader.loadBTreeNode(children.get(children.size()-1), this.dictionary).search(key);
	}


	

	
	@Override
	public byte[] getCorrespondKey(int i) {
		// TODO Auto-generated method stub
		NodeKeyPos nodePos = positions.get(i);
		long pos = nodePos.inNodePos;
		int keySize = nodePos.keyLength;
		//System.out.println("node number:" + this.NodeNumber);
		try {
			if (pos + keySize <= BTreeLeafNode.DEFAULT_DATA_SIZE&& data != null && pos+keySize <= data.size()) {
				List<Byte> lst  = data.subList((int)pos, (int)pos+keySize);
				byte[] keyBytes = new byte[keySize];
				for(int j = 0; j < keySize; j++) {
					keyBytes[j] = lst.get(j);
				}
				return keyBytes;

			} else {
				//System.out.println(data.size());
				return BufferedReader.getCorrespondFileByteArr(this.nodeFile, pos, keySize);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	
	
}
