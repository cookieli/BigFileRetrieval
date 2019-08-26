package lzx.retrieval;

public class KVPair {
	
	public byte[] elem1;
	public byte[] elem2;
	
	public int size;
	
	private void setSize() {
		size = elem1.length + elem2.length;
	}
	
	public KVPair(byte[] element1, byte[] element2) {
		this.elem1 = element1;
		this.elem2 = element2;
		setSize();
		
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
	
	public static boolean compare(byte[] key1, byte[] key2) {
		if(key1.length != key2.length) {
			return key1.length < key2.length;
		} 
		for(int i = 0; i < key1.length; i++) {
			if(key1[i] > key1[2]) {
				return false;
			}
		}
		return true;
	}
	
}
