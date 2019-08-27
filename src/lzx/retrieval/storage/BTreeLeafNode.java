package lzx.retrieval.storage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;

import lzx.retrieval.BufferedReader;
import lzx.retrieval.KVPair;

public class BTreeLeafNode extends BTreeNode{
	
	public ArrayList<Long> vars;
	
	public byte[] kvs;
	
	
	public BTreeLeafNode() {
		vars = new ArrayList<>();
		
		curNodeSize = 0;
		setNodeNumber();
		father = -1;
		isLeaf = true;
		isInternal = false;
		rightFatherEntry = -1;
	}
	public BTreeLeafNode(ArrayList<Long> vars) {
		this.vars = vars;
		
		setNodeNumber();
		father = -1;
		isLeaf = true;
		isInternal = false;
		rightFatherEntry = -1;
	}
	
	public void addKey(Long kv, String fileName) throws IOException {
		KVPair o1 = BufferedReader.getCorrespondKV(fileName, kv);
		for(int i = 0 ; i < vars.size(); i++) {
			KVPair o2 = BufferedReader.getCorrespondKV(fileName, vars.get(i));
			if(o1.compareTo(o2) < 0) {
				vars.add(i, kv);
				curNodeSize += 8;
				return;
			}
		}
		vars.add(kv);
		curNodeSize += 8;
	}

	@Override
	public BTreeNode[] split() {
		// TODO Auto-generated method stub
		int mid = this.entryNum()/2;
		Long p = vars.get(mid);
		BTreeLeafNode child1 = new BTreeLeafNode(new ArrayList<Long>(vars.subList(mid+1, vars.size())));
		child1.flushDataToFile(this.nodeFile);
		this.vars = new ArrayList<Long>(vars.subList(0, mid));
		if(this.NodeNumber == 0) {
			File file = new File(this.nodeFile);
			setNodeNumber();
			File other = new File(this.nodeFile);
			if(!file.renameTo(other)) {
				System.out.println("can't rename file");
				System.exit(-1);
			}
		}
		BTreeInternalNode father = new BTreeInternalNode(p, this.NodeNumber, child1.NodeNumber);
		child1.father = this.father;
		father.rightFatherEntry = this.rightFatherEntry;
		if(this.rightFatherEntry == -1) {
			this.rightFatherEntry = 0;
		} 
		child1.rightFatherEntry = this.rightFatherEntry + 1;
		
		return new BTreeNode[] {father, this, child1};
	}

	@Override
	public void merge(BTreeNode NodeA, BTreeNode NodeB) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public byte[] toByteArray() {
		// TODO Auto-generated method stub
		return null;
	}
	
	
	public String toString(String fileName) {
		StringBuilder sb = new StringBuilder();
		sb.append("leafNode " + this.NodeNumber +"\n");
		for(Long p: vars) {
			try {
				sb.append(BufferedReader.getCorrespondKV(fileName, p));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			sb.append("\n");
		}
		sb.append("\n");
		return sb.toString();
	}
	@Override
	public int entryNum() {
		// TODO Auto-generated method stub
		return this.vars.size();
	}
	@Override
	public KVPair search(byte[] key, String fileName) {
		// TODO Auto-generated method stub
		for(Long k: vars) {
			try {
				KVPair kv = BufferedReader.getCorrespondKV(fileName, k);
				if(Arrays.equals(kv.elem1, key)) {
					return kv;
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return null;
	}
	@Override
	public void flushDataToFile(String srcFile) {
		// TODO Auto-generated method stub
		File file = new File(this.nodeFile);
		try {
			if(!file.exists()) {
				file.createNewFile();
			}
			long curPos = 0;
			RandomAccessFile raf = new RandomAccessFile(file, "rwd");
			for(int i = 0; i < vars.size(); i++) {
				Long pos = vars.get(i);
				KVPair kv;
				kv = BufferedReader.getCorrespondKV(srcFile, pos);
				raf.write(kv.tobyteArray());
				vars.set(i,  curPos);
				curPos += kv.size;
			}
			raf.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
}
