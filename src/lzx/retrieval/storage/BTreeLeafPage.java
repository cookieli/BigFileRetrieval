package lzx.retrieval.storage;

import java.util.ArrayList;

import lzx.retrieval.KVPair;

public class BTreeLeafPage extends BTreePage{
	
	public ArrayList<KVPair> vars;
	
	
	
	
	public BTreeLeafPage() {
		vars = new ArrayList<>();
		entryNum = 0;
		curPageSize = 0;
		pageNumber = pageCnt;
		pageCnt++;
		father = -1;
		isLeaf = true;
		isInternal = false;
		rightFatherEntry = -1;
	}
	public BTreeLeafPage(ArrayList<KVPair> vars) {
		this.vars = vars;
		entryNum = vars.size();
		pageNumber = assignPageNumber();
		father = -1;
		isLeaf = true;
		isInternal = false;
		rightFatherEntry = -1;
	}
	
	public void addKey(KVPair kv) {
		vars.add(kv);
		curPageSize += kv.size;
	}

	@Override
	public BTreePage[] split() {
		// TODO Auto-generated method stub
		int mid = this.entryNum/2;
		KVPair p = vars.get(mid);
		BTreeLeafPage child1 = new BTreeLeafPage(new ArrayList<KVPair>(vars.subList(mid, vars.size())));
		this.vars = new ArrayList<KVPair>(vars.subList(0, mid));
//		if(this.pageNumber == 0) {
//			this.pageNumber = BTreePage.assignPageNumber();
//		}
		BTreeInternalPage father = new BTreeInternalPage(p.elem1, this.pageNumber, child1.pageNumber);
		
		father.rightFatherEntry = this.rightFatherEntry;
		if(this.rightFatherEntry == -1) {
			this.rightFatherEntry = 0;
		} 
		child1.rightFatherEntry = this.rightFatherEntry + 1;
		return new BTreePage[] {father, this, child1};
	}

	@Override
	public void merge(BTreePage pageA, BTreePage pageB) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public byte[] toByteArray() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("leafPage " + this.pageNumber +"\n");
		for(KVPair p: vars) {
			sb.append(p.toString());
			sb.append("\n");
		}
		sb.append("\n");
		return sb.toString();
	}
	
}
