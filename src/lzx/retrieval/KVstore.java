package lzx.retrieval;

public interface KVstore {
	
	public byte[] read(byte[] key);
	public void loadStoreFile(String file);
	public void close();
}
