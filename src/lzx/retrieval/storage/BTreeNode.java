package lzx.retrieval.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import lzx.retrieval.BufferedReader;
import lzx.retrieval.ByteUtils;
import lzx.retrieval.KVPair;
import lzx.retrieval.storage.BTreeLeafNode.NodeKeyPos;

public abstract class BTreeNode {
	public static final int DEFAULT_Node_SIZE = 512 * 1024;
	public static int DEFAULT_DATA_SIZE = 10;
	public static final int DEFAULT_B_ORDER = 3;
	public static final String nodeSuffix = ".nd";
	public boolean isDirty = false;
	public static int NodeCnt = 0;
	public long curNodeSize;
	public int NodeNumber = -1;
	public String nodeFile = null;
	public String nodeTreeFile = null;
	public String dictionary  = null;
	public String getDictionary() {
		return dictionary;
	}

	public void setDictionary(String dictionary) {
		this.dictionary = dictionary;
		setNodeFile();
	}

	public boolean hasNodeFile;
	public boolean isLeaf;
	public boolean isHeader;
	public boolean isInternal;
	public int father;
	public int cnt;
	// public ArrayList<Byte> data;
	// public ArrayList<NodeKeyPos> positions;

	public BTreeNode() {

	}

	public BTreeNode(ArrayList<NodeKeyPos> poSs, BTreeNode n, boolean copy, String dictionary) {
		this.positions = new ArrayList<>();
		int size = poSs.size();
		data = new ArrayList<>();
		this.nodeFile = "tmp.nd";
		setDictionary(dictionary);
		for (int i = 0; i < size; i++) {
			Long kv = poSs.get(i).originFilePos;
			byte[] key = n.getCorrespondKey(poSs.get(i));
			try {
				this.addKey(kv, key, false);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		File file = new File(this.nodeFile);
		String oldFile = n.nodeFile;
		if (n.NodeNumber == 0) {
			n.setNodeNumber();
			// System.out.println(n.NodeNumber);
		}
		if (file.exists()) {
			File originFile = new File(oldFile);
			if (originFile.exists()) {
				originFile.delete();
			}
			if (!file.renameTo(new File(n.nodeFile))) {
				System.out.println("rename fails");
				System.out.println(n.nodeFile);
			}
		}
		this.NodeNumber = n.NodeNumber;
		setNodeFile();
	}

	public BTreeNode(ArrayList<NodeKeyPos> poSs, BTreeNode n,String dict) {
		this.positions = new ArrayList<>();
		int size = poSs.size();
		data = new ArrayList<>();
		setNodeNumber();
		setDictionary(dict);
		for (int i = 0; i < size; i++) {
			Long kv = poSs.get(i).originFilePos;
			byte[] key = n.getCorrespondKey(poSs.get(i));
			try {
				this.addKey(kv, key, false);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void addKey(byte[] key) {
		if (curNodeSize + key.length > DEFAULT_DATA_SIZE) {
			addByteToFile(key, curNodeSize);
		} else {
			if(data == null) {
				data = new ArrayList<>();
			}
			for (int i = 0; i < key.length; i++) {
				data.add(key[i]);
			}
		}
		curNodeSize += key.length;
	}

	public void addKey(Long kv, byte[] key, boolean hasData) throws IOException {
		if (positions == null) {
			System.out.println("not initialize");
		}
		if (hasData) {
			for (int i = 0; i < positions.size(); i++) {
				byte[] nodeKey = getCorrespondKey(i);
				if (KVPair.compare(key, nodeKey) < 0) {
					positions.add(i, new NodeKeyPos(curNodeSize, key.length, kv));
					addKey(key);
					return;
				}
			}
		}
		positions.add(new NodeKeyPos(curNodeSize, key.length, kv));
		addKey(key);
	}

	public ArrayList<Byte> data;
	public ArrayList<NodeKeyPos> positions;

	public int getFather() {
		return father;
	}

	public void setNodeFile() {
		nodeFile = this.dictionary+"/"  + NodeNumber + nodeSuffix;
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

	public int rightFatherEntry;// it is father's larger than entry

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
		if (this.nodeFile != null) {
			File file = new File(this.nodeFile);
			if (file.exists()) { 
				// setNodeFile();
				setNodeFile();
				file.renameTo(new File(this.nodeFile));
			}
			setNodeFile();
		}else {
			setNodeFile();
		}

		this.father = -1;

	}

	public boolean isInternal() {
		return isInternal;
	}

	public void setInternal(boolean isInternal) {
		this.isInternal = isInternal;
	}

	public abstract long search(byte[] key);

	public abstract int entryNum();

	public abstract BTreeNode[] split();

	public abstract void merge(BTreeNode NodeA, BTreeNode NodeB);

	public abstract byte[] getCorrespondKey(int pos);

	public int getNodeNumber() {
		return NodeNumber;
	}

	public void setNodeNumber(int NodeNumber) {
		this.NodeNumber = NodeNumber;
	}

	public byte[] toByteArray() {
		// TODO Auto-generated method stub
		byte[] res = new byte[DEFAULT_Node_SIZE];
		cnt = 0;
		res[cnt++] = ByteUtils.boolToByte(this.isLeaf);
		res[cnt++] = ByteUtils.boolToByte(this.isInternal);
		byte[] bytes = ByteUtils.intToBytes(this.NodeNumber);
		for (int i = 0; i < bytes.length; i++) {
			res[cnt++] = bytes[i];
		}
		bytes = ByteUtils.intToBytes(this.father);
		for (int i = 0; i < bytes.length; i++) {
			res[cnt++] = bytes[i];
		}
		bytes = ByteUtils.intToBytes(this.rightFatherEntry);
		for (int i = 0; i < bytes.length; i++) {
			res[cnt++] = bytes[i];
		}
		bytes = ByteUtils.longToBytes(this.curNodeSize);
		for (int i = 0; i < bytes.length; i++) {
			res[cnt++] = bytes[i];
		}
		bytes = ByteUtils.intToBytes(this.entryNum());
		for (int i = 0; i < bytes.length; i++) {
			res[cnt++] = bytes[i];
		}
		for (NodeKeyPos pos : positions) {
			bytes = pos.tobyte();
			for (int i = 0; i < bytes.length; i++) {
				res[cnt++] = bytes[i];
			}
		}
		return res;
	}

	public static BTreeNode generateBTreeNodes(byte[] bytes, String TreeDictionary) {
		int cnt = 0;
		boolean isLeaf = ByteUtils.byteTobool(bytes[cnt++]);
		boolean isInternal = ByteUtils.byteTobool(bytes[cnt++]);
		if (isLeaf) {
			BTreeLeafNode leaf = new BTreeLeafNode(TreeDictionary);
			leaf.restoreFromByteArr(bytes);
			return leaf;
		}
		if (isInternal) {
			BTreeInternalNode internal = new BTreeInternalNode(TreeDictionary);
			internal.restoreFromByteArr(bytes);
			return internal;
		}
		return null;

	}

	public void restoreFromByteArr(byte[] bytes) {
		// TODO Auto-generated method stub
		cnt = 0;
		this.isLeaf = ByteUtils.byteTobool(bytes[cnt++]);
		this.isInternal = ByteUtils.byteTobool(bytes[cnt++]);
		this.NodeNumber = ByteUtils.bytesToInt(Arrays.copyOfRange(bytes, cnt, cnt + Integer.BYTES));
		cnt += Integer.BYTES;
		this.father = ByteUtils.bytesToInt(Arrays.copyOfRange(bytes, cnt, cnt + Integer.BYTES));
		cnt += Integer.BYTES;
		this.rightFatherEntry = ByteUtils.bytesToInt(Arrays.copyOfRange(bytes, cnt, cnt + Integer.BYTES));
		cnt += Integer.BYTES;
		this.curNodeSize = ByteUtils.bytesToLong(Arrays.copyOfRange(bytes, cnt, cnt + Long.BYTES));
		cnt += Long.BYTES;
		int entryNum = ByteUtils.bytesToInt(Arrays.copyOfRange(bytes, cnt, cnt + Integer.BYTES));
		cnt += Integer.BYTES;
		this.positions = new ArrayList<>();
		for (int i = 0; i < entryNum; i++) {
			this.positions
					.add(NodeKeyPos.generateNodeKeyPos(Arrays.copyOfRange(bytes, cnt, cnt + NodeKeyPos.byteNum())));
			cnt += NodeKeyPos.byteNum();
		}
		setDictionary(this.dictionary);

	}

	public void flushTreeToFile() {
		this.nodeTreeFile = dictionary +"/" + this.NodeNumber + ".tree";
		File file = new File(this.nodeTreeFile);
		try {
			if (!file.exists()) {
				if(!file.createNewFile()) {
					System.out.println("can't create");
					System.exit(-1);
				}
			}
			RandomAccessFile raf = new RandomAccessFile(file, "rwd");
			raf.write(this.toByteArray());
			raf.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	public void flushDataToFile() {
		// TODO Auto-generated method stub
		File file = new File(this.nodeFile);
		try {
			if (!file.exists()) {
				file.getParentFile().mkdir();
				if (!file.createNewFile()) {
					System.out.println("can't create");
					System.exit(-1);
				}
			} else {
				return;
			}
			RandomAccessFile raf = new RandomAccessFile(file, "rwd");

			for (int i = 0; i < BTreeLeafNode.DEFAULT_DATA_SIZE && i < data.size(); i++) {
				raf.write(data.get(i));
			}
			raf.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

	}

	public void addByteToFile(byte[] bytes, long pos) {
		File file = new File(this.nodeFile);
		if (!file.exists()) {
			flushDataToFile();
		}
		try {
			RandomAccessFile raf = new RandomAccessFile(file, "rwd");
			raf.seek(pos);
			raf.write(bytes);
			raf.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void resetChildNode(ArrayList<NodeKeyPos> poSs) {
		this.positions = poSs;
		if (this.NodeNumber == 0) {
			File file = new File(this.nodeFile);
			setNodeNumber();
			if (file.exists()) {
				file.renameTo(new File(this.nodeFile));
			}
		}
	}

	public byte[] getCorrespondKey(NodeKeyPos nodePos) {
		long pos = nodePos.inNodePos;
		int keySize = nodePos.keyLength;
		try {
			if (pos + keySize <= DEFAULT_DATA_SIZE && pos + keySize <= data.size()) {
				List<Byte> lst;
				if (pos + keySize == DEFAULT_DATA_SIZE)
					lst = data.subList((int) pos, data.size());
				else
					lst = data.subList((int) pos, (int) pos + keySize);
				// System.out.println(keySize);
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

	public void deleteFile() {
		if (this.nodeFile.equals("tmp.nd")) {
			File file = new File(this.nodeFile);
			if (file.exists()) {
				file.delete();
			}
		}
	}

}
