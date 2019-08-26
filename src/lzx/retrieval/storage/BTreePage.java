package lzx.retrieval.storage;

public abstract class BTreePage {
	public static final int DEFAULT_PAGE_SIZE = 3;
	public static final int MINIMAL_B_ORDER = 3;
	public static int header;
	public static int pageCnt = 0;
	public int entryNum;
	public int curPageSize;
	public int pageNumber;
	public boolean isLeaf;
	public boolean isHeader;
	public boolean isInternal;
	public int father;
	public int getFather() {
		return father;
	}
	public void setFather(int father) {
		this.father = father;
	}
	
	public static int assignPageNumber() {
		return pageCnt++;
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
		BTreePage.header = this.pageNumber;
	}
	public boolean isInternal() {
		return isInternal;
	}
	public void setInternal(boolean isInternal) {
		this.isInternal = isInternal;
	}
	public abstract BTreePage[] split();
	public abstract void merge(BTreePage pageA, BTreePage pageB);
	public int getPageNumber() {
		return pageNumber;
	}
	public void setPageNumber(int pageNumber) {
		this.pageNumber = pageNumber;
	}
	public abstract byte[] toByteArray();
}
