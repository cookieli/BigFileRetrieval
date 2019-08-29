package lzx.retrieval;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import lzx.retrieval.storage.BTreeInternalNode;
import lzx.retrieval.storage.BTreeLeafNode;
import lzx.retrieval.storage.BTreeNode;

public class BufferedReader {

	public static final int DEFAULT_Node_SIZE = 10;
	public static final int DEFAULT_Node_NUM = 5;

	public boolean hasReadAllFile;
	public boolean bufferIsEmpty;
	public String fileName;
	public String TreeDictionary;
	public String posFile;
	public RandomAccessFile raf;
	public FileChannel channel;
	public ByteBuffer buff;
	public long lastPosition = 0;
	public long position = 0;
	public byte[] data;

	public static LRUcache Nodes;

	public int BufferoffSet = 0;
	public int bufferBound = 0;

	public String getCorrespondTreeFileName(String fileName, String suffix) {
		int index = fileName.lastIndexOf(".");
		String name = fileName.substring(0, index);
		return name + suffix;
	}

	public BufferedReader(String name, long pos) throws IOException {
		fileName = name;
		this.position = pos;
		raf = new RandomAccessFile(fileName, "r");
		raf.seek(pos);
		channel = raf.getChannel();
		buff = ByteBuffer.allocate(DEFAULT_Node_SIZE);
		TreeDictionary = fileName.substring(0, fileName.lastIndexOf(".")) + "Tree";// getCorrespondTreeFileName(fileName,
																					// ".tree");
		posFile = TreeDictionary + "/" + "pos.data";
		// File f = new File(BTreeFileName);
		Nodes = new LRUcache(DEFAULT_Node_NUM);
		hasReadAllFile = false;
		bufferIsEmpty = false;
		getNxtNode();
		
	}

	public void close() {
		Nodes.clear();
		File file = new File(this.posFile);
		try {
			if (!file.exists()) {
				file.getParentFile().mkdir();

				if (!file.createNewFile()) {
					System.out.println("can't create");
					System.exit(-1);
				}

			}
			RandomAccessFile raf = new RandomAccessFile(this.posFile, "rw");
			raf.writeLong(position);
			raf.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static KVPair getCorrespondKV(String file, long pos) throws IOException {
		RandomAccessFile raf = new RandomAccessFile(file, "r");
		raf.seek(pos);
		byte[] intBytes = new byte[4];
		raf.read(intBytes);
		int keySize = ByteBuffer.wrap(intBytes).getInt();
		byte[] keyBytes = new byte[keySize];
		raf.read(keyBytes);
		raf.read(intBytes);
		int valueSize = ByteBuffer.wrap(intBytes).getInt();
		byte[] valueBytes = new byte[valueSize];
		raf.read(valueBytes);
		raf.close();
		return new KVPair(keyBytes, valueBytes);

	}

	public static byte[] getOriginFileByteArr(String file, long pos) throws IOException {
		RandomAccessFile raf = new RandomAccessFile(file, "r");
		raf.seek(pos);
		byte[] intBytes = new byte[4];
		raf.read(intBytes);
		int keySize = ByteBuffer.wrap(intBytes).getInt();
		byte[] keyBytes = new byte[keySize];
		raf.read(keyBytes);
		raf.close();
		return keyBytes;
	}

	public static byte[] getCorrespondFileByteArr(String file, long pos, int keySize) throws IOException {
		RandomAccessFile raf = new RandomAccessFile(file, "r");
		raf.seek(pos);
		byte[] keyBytes = new byte[keySize];
		raf.read(keyBytes);
		raf.close();
		return keyBytes;
	}

	public void flowDataUp(BTreeNode node) {
		while (node.entryNum() > BTreeNode.DEFAULT_B_ORDER) {
			BTreeNode[] splitNodes = node.split();
			if (node.father != -1) {
				BTreeInternalNode fatherNode = (BTreeInternalNode) loadBTreeNode(node.father, this.TreeDictionary);
				fatherNode.merge(fatherNode, splitNodes[0]);
				node = fatherNode;
				storeBTreeNode(splitNodes[2]);
				storeBTreeNode(splitNodes[1]);
			} else {
				splitNodes[0].setHeader(true);
				// splitNodes[0].setNodeNumber(0);
				splitNodes[1].setFather(0);
				splitNodes[2].setFather(0);
				// System.out.println(splitNodes[0]);
				for (BTreeNode p : splitNodes) {
					storeBTreeNode(p);
				}
				return;
			}
		}
	}

	public void storeKVIntoLeaf(byte[] bytes, BTreeLeafNode leaf) throws IOException {
		leaf.addKey(lastPosition, bytes, true);
		flowDataUp(leaf);
	}

	public void flowKVIntoInternal(byte[] bytes, BTreeInternalNode Node) throws IOException {
		int i;
		for (i = 0; i < Node.positions.size(); i++) {
			try {
				if (KVPair.compare(bytes, Node.getCorrespondKey(i)) < 0) {
					storeKVIntoNode(bytes, Node.children.get(i));
					return;
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		storeKVIntoNode(bytes, Node.children.get(Node.positions.size()));
	}

	public void storeKVIntoNode(byte[] bytes, int NodeId) throws IOException {
		if (NodeId == -1) {
			BTreeLeafNode leaf = new BTreeLeafNode(this.TreeDictionary);
			leaf.setHeader(true);
			leaf.addKey(lastPosition, bytes, true);
			storeBTreeNode(leaf);
		} else {
			BTreeNode Node = loadBTreeNode(NodeId, this.TreeDictionary);
			if (Node.isLeaf) {
				storeKVIntoLeaf(bytes, (BTreeLeafNode) Node);
			} else {
				flowKVIntoInternal(bytes, (BTreeInternalNode) Node);
			}
		}
	}

	public static BTreeNode loadBTreeNode(int NodeId, String dict) {
		if (Nodes.containsKey(NodeId)) {
			return Nodes.get(NodeId);
		} else {
			// System.out.println("in this");
			File treeFile = new File(dict + "/" + NodeId + ".tree");
			if (treeFile.exists()) {
				try {
					byte[] content = Files.readAllBytes(treeFile.toPath());
					return BTreeNode.generateBTreeNodes(content, dict);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			System.out.println("happens in loadBTreeNode");
			return null;
		}
		// return null;
	}

	public void storeBTreeNode(BTreeNode Node) {
		// Node.setDictionary(this.TreeDictionary);
		Nodes.put(Node.NodeNumber, Node);

	}

	public long get(byte[] key) throws IOException {
		if (!Nodes.isEmpty() || position>0) {
			long res = loadBTreeNode(0, this.TreeDictionary).search(key);
			if (res != -1)
				return res;

		}
		if (bufferIsEmpty) {
			//System.out.println("in get false");
			return -1;
		}
		byte[] bytes = getNextKey();
		if (Nodes.isEmpty()) {
			storeKVIntoNode(bytes, -1);
		} else {
			storeKVIntoNode(bytes, 0);
		}
		while (!Arrays.equals(key, bytes)) {
			bytes = getNextKey();
			if (bytes == null)
				break;
			storeKVIntoNode(bytes, 0);
		}
		return lastPosition;
	}

	public void getNxtNode() {
		try {
			if (channel.read(buff) == 0 || channel.read(buff) == -1)
				hasReadAllFile = true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		out.write(buff.array(), 0, buff.position());
		buff.clear();
		data = out.toByteArray();
		bufferBound = data.length;
		if (hasReadAllFile) {
			if (bufferBound == 0)
				bufferIsEmpty = true;
		}
		BufferoffSet = 0;
	}

	private int getSize() {
		return ByteBuffer.wrap(getValue(4)).getInt();
	}

	private byte[] getValue(int length) {
		if (BufferoffSet >= bufferBound) {
			getNxtNode();
		} else if (BufferoffSet + length >= bufferBound) {
			byte[] temp = new byte[length];
			int cnt = 0;
			for (int i = BufferoffSet; i < bufferBound; i++) {
				temp[i - BufferoffSet] = data[i];
				cnt = i - BufferoffSet;
			}
			getNxtNode();
			for (int i = cnt + 1; i < length; i++) {
				temp[i] = data[BufferoffSet++];
			}
			return temp;

			// return ByteBuffer.wrap(temp).getInt();
		}
		int prev = BufferoffSet;
		BufferoffSet += length;
		return Arrays.copyOfRange(data, prev, BufferoffSet);
	}

	public byte[] getNextKey() {
		if (bufferIsEmpty)
			return null;
		int key_Size = getSize();
		// System.out.println(key_Size);
		byte[] key = getValue(key_Size);
		int valueSize = getSize();
		getValue(valueSize);
		lastPosition = position;
		position += 8 + key.length + valueSize;
		if (hasReadAllFile && BufferoffSet == bufferBound) {
			bufferIsEmpty = true;
		}
		return key;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		Queue<BTreeNode> queue = new LinkedList<>();
		BTreeNode Node = loadBTreeNode(0, this.TreeDictionary);
		// System.out.println("header"+ Node.toString());
		queue.add(Node);
		while (!queue.isEmpty()) {
			BTreeNode p = queue.remove();
			if (p == null)
				return sb.toString();
			if (p.isInternal) {

				BTreeInternalNode pI = (BTreeInternalNode) p;
				// System.out.println(pI.toString());
				for (int i : pI.children) {
					// System.out.println(pI.children.get(i));
					queue.add(loadBTreeNode(i, this.TreeDictionary));
				}
			}
			sb.append(p.toString());
		}
		return sb.toString();
	}

	public static void main(String[] args) throws IOException {
		java.io.BufferedReader br = new java.io.BufferedReader(
				new FileReader("/home/lzx/eclipse-workspace/BigFileRetrieval/tests/a.txt"));
		String outFile = "/home/lzx/eclipse-workspace/BigFileRetrieval/tests/a.data";
		String line = br.readLine();
		System.out.println(line);
		FileOutputStream out = new FileOutputStream(outFile);
		while (line != null) {
			String[] str = line.split(",");
			// System.out.println(str[0].trim());
			int keySize = Integer.parseInt(str[0].trim());
			byte[] keySizeBytes = ByteBuffer.allocate(4).putInt(keySize).array();
			byte[] keyByte = str[1].trim().getBytes(StandardCharsets.UTF_8);
			// System.out.println(new String(keyByte));
			int valueSize = Integer.parseInt(str[2].trim());
			byte[] valueSizeBytes = ByteBuffer.allocate(4).putInt(valueSize).array();
			byte[] valueBytes = str[3].trim().getBytes(StandardCharsets.UTF_8);
//		File f = new File(outFile);
//		f.createNewFile();

//		for(int i = 0; i < keyByte.length; i++) {
//			System.out.print(keyByte[i] + " ");
//		}
			// System.out.println();
			out.write(keySizeBytes);
			out.write(keyByte);
			out.write(valueSizeBytes);
			out.write(valueBytes);
			out.flush();
			line = br.readLine();
		}

//		RandomAccessFile f = new RandomAccessFile(outFile, "r");
//		byte[] b = new byte[(int)f.length()];
//		f.readFully(b);
//		for(int i = 0; i < b.length; i++) {
//			System.out.println(b[i]);
//		}
		BufferedReader r = new BufferedReader(outFile, 0);
		System.out.println(new String(r.getNextKey()));

	}

}
