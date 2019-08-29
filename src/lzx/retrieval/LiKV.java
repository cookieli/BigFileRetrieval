package lzx.retrieval;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class LiKV implements KVstore {

	public BufferedReader br;
	String fileName;
	
	public LiKV() {
		
	}

	public LiKV(String name) throws IOException {
		this.fileName = name;
		br = new BufferedReader(name, 0);
	}

	@Override
	public byte[] read(byte[] key) {
		// TODO Auto-generated method stub
		long kv;
		try {
			kv = br.get(key);
			if (kv != -1)
				return BufferedReader.getCorrespondKV(this.fileName, kv).elem2;

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	

	@Override
	public void loadStoreFile(String file) {
		// TODO Auto-generated method stub
		String dictinary = file.substring(0, file.lastIndexOf(".")) + "Tree";
		String posFile = dictinary + "/" + "pos.data";
		File posfile = new File(posFile);
		try {
			if (posfile.exists()) {

				RandomAccessFile raf = new RandomAccessFile(posfile, "rw");
				long pos = raf.readLong();
				
				br = new BufferedReader(file, pos);
				raf.close();
			} else {
				br = new BufferedReader(file, 0);
			}
			this.fileName = file;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		br.close();

	}
	public static void main(String[] args) throws IOException {
		LiKV kv = new LiKV();
		kv.loadStoreFile("tests/a.data");
		byte[] key = { 'h' };
		byte[] res = kv.read(key);
		if (res != null)
			System.out.println(new String(res));
		else
			System.out.println("null");
		System.out.println(kv.br.toString());
		kv.close();
		//kv.loadStoreFile("tests/a.data");
//		byte[] key2 = { 'w' };
//		res = kv.read(key2);
//		if (res != null)
//			System.out.println(new String(res));
//		else
//			System.out.println("null");
	}

}
