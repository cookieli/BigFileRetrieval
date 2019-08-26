package lzx.retrieval.storage;

import java.util.ArrayList;

import lzx.retrieval.KVPair;

public class BTreeInternalPage extends BTreePage{
	
	public ArrayList<byte[]> keys;
	
	public ArrayList<Integer> children;
	
	public BTreeInternalPage(byte[] key, int ch1, int ch2) {
		keys = new ArrayList<>();
		children = new ArrayList<>();
		isInternal = true;
		keys.add(key);
		children.add(ch1);
		children.add(ch2);
	}
	
	public BTreeInternalPage(ArrayList<byte[]> keys, ArrayList<Integer> children) {
		this.keys = keys;
		this.children = children;
		isInternal = true;
		
	}
	

	@Override
	public BTreePage[] split() {
		// TODO Auto-generated method stub
		int mid = keys.size()/2;
		byte[] key = keys.get(mid);
		BTreeInternalPage child1 = new BTreeInternalPage(new ArrayList<byte[]>(keys.subList(mid+1, keys.size())),new ArrayList<Integer>(children.subList(mid+1, children.size())));
		this.keys = new ArrayList<byte[]>(this.keys.subList(0, mid));
		this.children = new ArrayList<Integer>(children.subList(0, mid+1));
		child1.pageNumber = BTreePage.assignPageNumber();
		BTreeInternalPage father = new BTreeInternalPage(key, this.pageNumber, child1.pageNumber);
		
		return new BTreePage[] {father, this, child1};
	}

	@Override
	public void merge(BTreePage parent, BTreePage child) {
		// TODO Auto-generated method stub
		BTreeInternalPage pa= (BTreeInternalPage) parent;
		BTreeInternalPage chi = (BTreeInternalPage) child;
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
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Internal page: " + pageNumber+"\n");
		for(byte[] key: keys) {
			sb.append(new String(key) +" ");
		}
		sb.append("\n");
		for(int i: children) {
			sb.append(i);
			sb.append(" ");
		}
		sb.append("\n");
		return sb.toString();
	}
	
}
