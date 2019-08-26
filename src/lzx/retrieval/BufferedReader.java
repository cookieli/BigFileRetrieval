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

import lzx.retrieval.storage.BTreeInternalPage;
import lzx.retrieval.storage.BTreeLeafPage;
import lzx.retrieval.storage.BTreePage;

public class BufferedReader {

	public static final int DEFAULT_PAGE_SIZE = 10;
	public static final int DEFAULT_PAGE_NUM = 1024;
	public boolean hasReadAllFile;
	public String fileName;
	public String BTreeFileName;
	public RandomAccessFile raf;
	public FileChannel channel;
	public ByteBuffer buff;

	public byte[] data;

	public Map<Integer, BTreePage> pages;

	public int BufferoffSet = 0;

	public String getCorrespondTreeFileName(String fileName) {
		int index = fileName.lastIndexOf(".");
		String name = fileName.substring(0, index);
		return name + ".tree";
	}

	public BufferedReader(String fileName) throws IOException {
		this.fileName = fileName;
		raf = new RandomAccessFile(this.fileName, "r");
		channel = raf.getChannel();
		buff = ByteBuffer.allocate(DEFAULT_PAGE_SIZE);
		BTreeFileName = getCorrespondTreeFileName(this.fileName);
		File f = new File(BTreeFileName);
		pages = new HashMap<>();
		if (!f.exists())
			f.createNewFile();
		getNxtPage();
	}
	
	public void storeKVIntoLeaf(KVPair kv, BTreeLeafPage leaf) {
		leaf.addKey(kv);
		if (leaf.curPageSize > BTreePage.DEFAULT_PAGE_SIZE) {
			BTreePage[] splitPages = leaf.split();
			if (pages.containsKey(leaf.father)) {
				
			} else {
				splitPages[0].setHeader(true);
				splitPages[0].setPageNumber(0);
				splitPages[1].setFather(0);
				splitPages[2].setFather(0);
				//System.out.println(splitPages[0]);
				for(BTreePage p: splitPages) {
					storeBTreePage(p);
				}
			}
		}
	}
	public void storeKVIntoInternal(KVPair kv, BTreeInternalPage page) {
		int i;
		for(i = 0; i < page.keys.size(); i++) {
			if(KVPair.compare(kv.elem1, page.keys.get(i))) {
				storeKVIntoPage(kv, page.children.get(i));
				return;
			}
		}
		storeKVIntoPage(kv, page.children.get(page.keys.size()));
	}
	public void storeKVIntoPage(KVPair kv, int pageId) {
		if (pageId == -1) {
			BTreeLeafPage leaf = new BTreeLeafPage();
			leaf.setHeader(true);
			leaf.addKey(kv);
			pages.put(0, leaf);
		} else {
			BTreePage page = loadBTreePage(0);
			if (page.isLeaf) {
				storeKVIntoLeaf( kv, (BTreeLeafPage) page);
			} else {
				storeKVIntoInternal(kv, (BTreeInternalPage)page);
			}
		}
	}

	public BTreePage loadBTreePage(int pageId) {
		if (pages.containsKey(pageId)) {
			return pages.get(pageId);
		}
		return null;
	}

	public void storeBTreePage(BTreePage page) {
		pages.put(page.pageNumber, page);
	}

	public KVPair get(byte[] key) {
		KVPair kv = getNextKV();
		if (pages.isEmpty()) {
			storeKVIntoPage(kv, -1);
		}
		while (!Arrays.equals(key, kv.elem1)) {
			kv = getNextKV();
			storeKVIntoPage(kv, 0);
		}
		return kv;
	}

	public void getNxtPage() {
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
		BufferoffSet = 0;
	}

	private int getSize() {
		return ByteBuffer.wrap(getValue(4)).getInt();
	}

	private byte[] getValue(int length) {
		if (BufferoffSet >= DEFAULT_PAGE_SIZE) {
			getNxtPage();
		} else if (BufferoffSet + length >= DEFAULT_PAGE_SIZE) {
			byte[] temp = new byte[length];
			int cnt = 0;
			for (int i = BufferoffSet; i < DEFAULT_PAGE_SIZE; i++) {
				temp[i - BufferoffSet] = data[i];
				cnt = i - BufferoffSet;
			}
			getNxtPage();
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

		int key_Size = getSize();
		// System.out.println(key_Size);
		byte[] key = getValue(key_Size);
		int valueSize = getSize();
		byte[] value = getValue(valueSize);

		return new KVPair(key, value);
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		Queue<BTreePage> queue = new LinkedList<>();
		BTreePage page = loadBTreePage(0);
		//System.out.println("header"+ page.toString());
		queue.add(page);
		while(!queue.isEmpty()) {
			BTreePage p = queue.remove();
			if(p.isInternal) {
				
				BTreeInternalPage pI = (BTreeInternalPage) p;
				//System.out.println(pI.toString());
				for(int i: pI.children) {
					//System.out.println(pI.children[i]);
					queue.add(loadBTreePage(i));
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
		BufferedReader r = new BufferedReader(outFile);
		System.out.println(r.getNextKV());

	}

}
