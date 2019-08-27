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
	public static final int DEFAULT_Node_NUM = 1024;
	
	public boolean hasReadAllFile;
	public boolean bufferIsEmpty;
	public String fileName;
	public String BTreeFileName;
	public RandomAccessFile raf;
	public FileChannel channel;
	public ByteBuffer buff;
	public long lastPosition = 0;
	public long position = 0;
	public byte[] data;

	public static Map<Integer, BTreeNode> Nodes;

	public int BufferoffSet = 0;
	public int bufferBound = 0;

	public String getCorrespondTreeFileName(String fileName, String suffix) {
		int index = fileName.lastIndexOf(".");
		String name = fileName.substring(0, index);
		return name + suffix;
	}
	
	

	public BufferedReader(String name) throws IOException {
		fileName = name;
		raf = new RandomAccessFile(fileName, "r");
		channel = raf.getChannel();
		buff = ByteBuffer.allocate(DEFAULT_Node_SIZE);
		BTreeFileName = getCorrespondTreeFileName(fileName, ".tree");
		File f = new File(BTreeFileName);
		Nodes = new HashMap<>();
		if (!f.exists()) {
			f.createNewFile();
			getNxtNode();
		} else if(f.length() == 0){
			getNxtNode();
		} else {
			
		}
		hasReadAllFile = false;
		bufferIsEmpty =false;
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
	
	public void flowDataUp(BTreeNode node) {
		while(node.entryNum() > BTreeNode.DEFAULT_Node_SIZE) {
			BTreeNode[] splitNodes = node.split();
			if(node.father != -1) {
				BTreeInternalNode fatherNode = (BTreeInternalNode) loadBTreeNode(node.father);
				fatherNode.merge(fatherNode, splitNodes[0]);
				node = fatherNode;
				storeBTreeNode(splitNodes[2]);
			} else {
				splitNodes[0].setHeader(true);
				//splitNodes[0].setNodeNumber(0);
				splitNodes[1].setFather(0);
				splitNodes[2].setFather(0);
				//System.out.println(splitNodes[0]);
				for(BTreeNode p: splitNodes) {
					storeBTreeNode(p);
				}
				return;
			}
		}
	}
	public void storeKVIntoLeaf(KVPair kv, BTreeLeafNode leaf) throws IOException {
		leaf.addKey(lastPosition, fileName);
		flowDataUp(leaf);
	}
	public void flowKVIntoInternal(KVPair kv, BTreeInternalNode Node) throws IOException {
		int i;
		for(i = 0; i < Node.keys.size(); i++) {
			try {
				if(KVPair.compare(kv.elem1, BufferedReader.getCorrespondKV(fileName,Node.keys.get(i)).elem1) < 0) {
					storeKVIntoNode(kv, Node.children.get(i));
					return;
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		storeKVIntoNode(kv, Node.children.get(Node.keys.size()));
	}
	public void storeKVIntoNode(KVPair kv, int NodeId) throws IOException {
		if (NodeId == -1) {
			BTreeLeafNode leaf = new BTreeLeafNode();
			leaf.setHeader(true);
			leaf.addKey(lastPosition, fileName);
			storeBTreeNode(leaf);
		} else {
			BTreeNode Node = loadBTreeNode(NodeId);
			if (Node.isLeaf) {
				storeKVIntoLeaf( kv, (BTreeLeafNode) Node);
			} else {
				flowKVIntoInternal(kv, (BTreeInternalNode)Node);
			}
		}
	}

	public static BTreeNode loadBTreeNode(int NodeId) {
		if (Nodes.containsKey(NodeId)) {
			return Nodes.get(NodeId);
		}
		return null;
	}

	public void storeBTreeNode(BTreeNode Node) {
		Nodes.put(Node.NodeNumber, Node);
		
	}
	
	public KVPair get(byte[] key) throws IOException {
		if(!Nodes.isEmpty()) {
			KVPair res = loadBTreeNode(0).search(key, fileName);
			if(res!= null)
				return res;
			
		}
		if(bufferIsEmpty) {
			return null;
		}
		KVPair kv = getNextKV();
		if (Nodes.isEmpty()) {
			storeKVIntoNode(kv, -1);
		}
		while (!Arrays.equals(key, kv.elem1)) {
			kv = getNextKV();
			if(kv == null) break;
			storeKVIntoNode(kv, 0);
		}
		return kv;
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
		if(hasReadAllFile) {
			if(bufferBound == 0)
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

	public KVPair getNextKV() {
		if(bufferIsEmpty)
			return null;
		int key_Size = getSize();
		// System.out.println(key_Size);
		byte[] key = getValue(key_Size);
		int valueSize = getSize();
		byte[] value = getValue(valueSize);
		lastPosition = position;
		position += 8+ key.length + value.length;
		if(hasReadAllFile && BufferoffSet == bufferBound) {
			bufferIsEmpty = true;
		}
		return new KVPair(key, value);
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		Queue<BTreeNode> queue = new LinkedList<>();
		BTreeNode Node = loadBTreeNode(0);
		//System.out.println("header"+ Node.toString());
		queue.add(Node);
		while(!queue.isEmpty()) {
			BTreeNode p = queue.remove();
			if(p == null) return sb.toString();
			if(p.isInternal) {
				
				BTreeInternalNode pI = (BTreeInternalNode) p;
				//System.out.println(pI.toString());
				for(int i: pI.children) {
					//System.out.println(pI.children.get(i));
					queue.add(loadBTreeNode(i));
				}
			}
			sb.append(p.toString(this.fileName));
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
		BufferedReader r = new BufferedReader(outFile);
		System.out.println(r.getNextKV());

	}

}
