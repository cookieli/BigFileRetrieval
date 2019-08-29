package lzx.retrieval.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import lzx.retrieval.BufferedReader;

public class BTreeLeafNode extends BTreeNode {

	public static class NodeKeyPos {
		long inNodePos;
		int keyLength;
		long originFilePos;

		public NodeKeyPos() {

		}
		
		public static  int byteNum() {
			return Long.BYTES + Integer.BYTES + Long.BYTES;
		}

		public NodeKeyPos(long inNodePos, int keyLength, long originFilePos) {
			this.inNodePos = inNodePos;
			this.keyLength = keyLength;
			this.originFilePos = originFilePos;
		}

		public String toString() {
			return "" + inNodePos + " " + keyLength + " " + originFilePos;
		}

		public byte[] tobyte() {
			byte[] res = new byte[Long.BYTES + Long.BYTES + Integer.BYTES];
			ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
			buffer.putLong(inNodePos);
			byte[] arr = buffer.array();
			int cnt = 0;
			for (int i = 0; i < Long.BYTES; i++) {
				res[cnt++] = arr[i];
			}
			buffer = ByteBuffer.allocate(Integer.BYTES);
			buffer.putInt(keyLength);
			arr = buffer.array();
			for (int i = 0; i < Integer.BYTES; i++) {
				res[cnt++] = arr[i];
			}
			buffer = ByteBuffer.allocate(Long.BYTES);
			buffer.putLong(originFilePos);
			arr = buffer.array();
			for (int i = 0; i < Long.BYTES; i++) {
				res[cnt++] = arr[i];
			}
			return res;
		}

		public void restoreFromByte(byte[] arr) {
			int cnt = 0;
			ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
			buffer.put(Arrays.copyOfRange(arr, cnt, cnt + Long.BYTES));
			cnt += Long.BYTES;
			buffer.flip();// need flip
			this.inNodePos = buffer.getLong();
			buffer = ByteBuffer.allocate(Integer.BYTES);
			buffer.put(Arrays.copyOfRange(arr, cnt, cnt + Integer.BYTES));
			cnt += Integer.BYTES;
			buffer.flip();// need flip
			this.keyLength = buffer.getInt();
			buffer = ByteBuffer.allocate(Long.BYTES);
			buffer.put(Arrays.copyOfRange(arr, cnt, cnt + Long.BYTES));
			cnt += Long.BYTES;
			buffer.flip();// need flip
			this.originFilePos = buffer.getLong();

		}
		public static NodeKeyPos generateNodeKeyPos(byte[] arr) {
			NodeKeyPos pos = new NodeKeyPos();
			pos.restoreFromByte(arr);
			return pos;
		}
	}

	public BTreeLeafNode(String dictionary) {
		positions = new ArrayList<>();
		data = new ArrayList<>();
		curNodeSize = 0;
		setNodeNumber();
		father = -1;
		isLeaf = true;
		isInternal = false;
		rightFatherEntry = -1;
		setDictionary(dictionary);
	}

	public BTreeLeafNode(ArrayList<NodeKeyPos> poSs, BTreeLeafNode n, String dictionary) {
		super(poSs, n, dictionary);

		father = -1;
		isLeaf = true;
		isInternal = false;
		rightFatherEntry = -1;
		//setDictionary(dictionary);
	}

	public BTreeLeafNode(ArrayList<NodeKeyPos> arrayList, BTreeLeafNode n,String dictionary,  boolean copy) {
		super(arrayList, n, copy,dictionary);
		father = -1;
		isLeaf = true;
		isInternal = false;
		rightFatherEntry = -1;
		//setDictionary(dictionary);
		// TODO Auto-generated constructor stub
	}

	@Override
	public BTreeNode[] split() {
		// TODO Auto-generated method stub
		int mid = this.entryNum() / 2;
		NodeKeyPos pos = this.positions.get(mid);
		byte[] bytes = this.getCorrespondKey(mid);
		BTreeLeafNode child1 = new BTreeLeafNode(
				new ArrayList<NodeKeyPos>(positions.subList(mid + 1, positions.size())), this, this.dictionary);
		//child1.setDictionary(this.dictionary);
		BTreeLeafNode child2 = new BTreeLeafNode(new ArrayList<NodeKeyPos>(positions.subList(0, mid)), this, this.dictionary, true);
		//child2.setDictionary(this.dictionary);
		// System.out.println(child1.toString());
		BTreeInternalNode father = new BTreeInternalNode(bytes, pos.originFilePos, child2.NodeNumber,
				child1.NodeNumber, this.dictionary);
		//father.setDictionary(this.dictionary);
		child1.father = this.father;
		child2.father = this.father;
		father.rightFatherEntry = this.rightFatherEntry;
		if (this.rightFatherEntry == -1) {
			this.rightFatherEntry = 0;
		}
		child2.rightFatherEntry = this.rightFatherEntry;
		child1.rightFatherEntry = this.rightFatherEntry + 1;
//		System.out.println("print" + father.toString());
//		System.out.println(child2);
//		System.out.println(child1);
		return new BTreeNode[] { father, child2, child1 };
	}

	@Override
	public void merge(BTreeNode NodeA, BTreeNode NodeB) {
		// TODO Auto-generated method stub

	}

	//pageNum, isLeaf, isInternal, father, rightFatherEntry, curNodeSize, entryNum, positions
	
	
	
	

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("leafNode " + this.NodeNumber + " "+ this.father+"\n");
		for (int i = 0; i < positions.size(); i++) {
			sb.append(i + " ");
			sb.append(new String(getCorrespondKey(i)));
			sb.append("\n");
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
		for (int i = 0; i < positions.size(); i++) {
			byte[] keyInNode = getCorrespondKey(i);
			if (Arrays.equals(keyInNode, key)) {
				return positions.get(i).originFilePos;
			}
		}
		return -1;
	}

	@Override
	public byte[] getCorrespondKey(int i) {
		// TODO Auto-generated method stub
		NodeKeyPos nodePos = positions.get(i);
		long pos = nodePos.inNodePos;
		int keySize = nodePos.keyLength;
		try {
			if (pos + keySize <= BTreeLeafNode.DEFAULT_DATA_SIZE && pos+keySize <= data.size()) {
				List<Byte> lst = data.subList((int) pos, (int) pos + keySize);
				byte[] keyBytes = new byte[keySize];
				for (int j = 0; j < keySize; j++) {
					keyBytes[j] = lst.get(j);
				}
				return keyBytes;

			} else {
				return BufferedReader.getCorrespondFileByteArr(this.nodeFile, pos, keySize);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args) {
		NodeKeyPos pos = new NodeKeyPos(5, 14, 3);
		NodeKeyPos temp = new NodeKeyPos();
		temp.restoreFromByte(pos.tobyte());
		System.out.println(temp);
	}

	
	


}
