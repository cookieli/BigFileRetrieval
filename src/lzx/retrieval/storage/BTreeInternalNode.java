package lzx.retrieval.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;

import lzx.retrieval.BufferedReader;
import lzx.retrieval.KVPair;

public class BTreeInternalNode extends BTreeNode{
	
	public ArrayList<Long> keys;
	
	public ArrayList<Integer> children;
	
	public int rightSiblingNode = -1;
	
	public BTreeInternalNode(Long key, int ch1, int ch2) {
		keys = new ArrayList<>();
		children = new ArrayList<>();
		isInternal = true;
		isLeaf = false;
		keys.add(key);
		children.add(ch1);
		children.add(ch2);
	}
	
	public BTreeInternalNode(ArrayList<Long> keys, ArrayList<Integer> children) {
		this.keys = keys;
		this.children = children;
		isInternal = true;
		isLeaf = false;
		setNodeNumber();
	}
	

	@Override
	public BTreeNode[] split() {
		// TODO Auto-generated method stub
		int mid = keys.size()/2;
		Long key = keys.get(mid);
		BTreeInternalNode child1 = new BTreeInternalNode(new ArrayList<Long>(keys.subList(mid+1, keys.size())),new ArrayList<Integer>(children.subList(mid+1, children.size())));
		child1.flushDataToFile(this.nodeFile);
		this.keys = new ArrayList<Long>(this.keys.subList(0, mid));
		this.children = new ArrayList<Integer>(children.subList(0, mid+1));
		if(this.NodeNumber == 0) {
			File file = new File(this.nodeFile);
			setNodeNumber();
			File other = new File(this.nodeFile);
			if(!file.renameTo(other)) {
				System.out.println("can't rename file");
				System.exit(-1);
			}
		}
		BTreeInternalNode father = new BTreeInternalNode(key, this.NodeNumber, child1.NodeNumber);
		
		return new BTreeNode[] {father, this, child1};
	}

	@Override
	public void merge(BTreeNode parent, BTreeNode child) {
		// TODO Auto-generated method stub
		BTreeInternalNode pa= (BTreeInternalNode) parent;
		BTreeInternalNode chi = (BTreeInternalNode) child;
		if(chi.rightFatherEntry >= pa.keys.size()) {
			pa.keys.add(chi.keys.get(0));
			pa.children.add(chi.children.get(chi.children.size() - 1));
		} else {
			pa.keys.add(chi.rightFatherEntry, chi.keys.get(0));
			pa.children.add(chi.rightFatherEntry+1, chi.children.get(chi.children.size() - 1));
		}
	}

	@Override
	public byte[] toByteArray() {
		// TODO Auto-generated method stub
		return null;
	}
	

	public String toString(String fileName)  {
		StringBuilder sb = new StringBuilder();
		sb.append("Internal Node: " + NodeNumber+"\n");
		for(Long key: keys) {
			try {
				sb.append(BufferedReader.getCorrespondKV(fileName, key) +" ");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
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
		return this.keys.size();
	}

	@Override
	public KVPair search(byte[] key, String fileName) {
		// TODO Auto-generated method stub
		int i= 0;
		for(Long k: keys) {
			try {
				KVPair kv = BufferedReader.getCorrespondKV(fileName, k);
				if(Arrays.equals(kv.elem1, key)) {
					return kv;
				} else if(KVPair.compare(key, kv.elem1) < 0) {
					return BufferedReader.loadBTreeNode(children.get(i)).search(key, fileName);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			i++;
		}
		
		return BufferedReader.loadBTreeNode(children.get(children.size()-1)).search(key, fileName);
	}

	@Override
	public void flushDataToFile(String srcFile) {
		// TODO Auto-generated method stub
		String fileName = ""+ this.NodeNumber + BTreeNode.nodeSuffix;
		File file = new File(fileName);
		try {
			if(!file.exists()) {
				file.createNewFile();
			}
			long curPos = 0;
			RandomAccessFile raf = new RandomAccessFile(file, "rwd");
			for(int i = 0; i < keys.size(); i++) {
				Long pos = keys.get(i);
				KVPair kv;
				kv = BufferedReader.getCorrespondKV(srcFile, pos);
				raf.write(kv.tobyteArray());
				keys.set(i,  curPos);
				curPos += kv.size;
			}
			raf.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
}
