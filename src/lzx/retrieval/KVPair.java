package lzx.retrieval;

import java.nio.ByteBuffer;

public class KVPair implements Comparable<KVPair>{
	
	public byte[] elem1;
	public byte[] elem2;
	
	public int size;
	
	private void setSize() {
		size = elem1.length + elem2.length +8;
	}
	
	public KVPair(byte[] element1, byte[] element2) {
		this.elem1 = element1;
		this.elem2 = element2;
		setSize();
		
	}
	
	public byte[] tobyteArray() {
		byte[] res = new byte[this.size];
		byte[] size = ByteBuffer.allocate(4).putInt(elem1.length).array();
		int pos = 0;
		for(int i = 0 ; i< 4; i++) {
			res[pos++] =size[i];
		}
		for(int i = 0; i < elem1.length; i++) {
			res[pos++] = elem1[i];
		}
		size = ByteBuffer.allocate(4).putInt(elem2.length).array();
		for(int i = 0 ; i< 4; i++) {
			res[pos++] =size[i];
		}
		
		for(int i = 0; i < elem2.length; i++) {
			res[pos++] = elem2[i];
		}
		
		return res;
		
	}
	
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("key: ");
		
		sb.append(new String(elem1));
		sb.append(" ");
		sb.append("value: ");
		sb.append(new String(elem2));
		
		return sb.toString();
	}
	
	public static int compare(byte[] key1, byte[] key2) {
		if(key1.length != key2.length) {
			return key1.length - key2.length;
		} 
		for(int i = 0; i < key1.length; i++) {
			if(key1[i] > key2[i]) {
				return 1;
			} else if(key1[i] < key2[i]) {
				return -1;
			}
		}
		return 0;
	}

	@Override
	public int compareTo(KVPair o) {
		// TODO Auto-generated method stub
		return compare(this.elem1, o.elem1);
	}
	
}
