package lzx.retrieval;

import java.io.IOException;

public class LiKV implements KVstore{
	
	
	public BufferedReader br;
	
	public LiKV(String name) throws IOException {
		br = new BufferedReader(name);
	}

	@Override
	public byte[] read(byte[] key) {
		// TODO Auto-generated method stub
		KVPair kv = br.get(key);
		return kv.elem2;
	}
	
	public static void main(String[] args) throws IOException {
		LiKV kv = new LiKV("/home/lzx/eclipse-workspace/BigFileRetrieval/tests/a.data");
		byte[] key = {'i'};
		System.out.println(new String(kv.read(key)));
		System.out.println(kv.br.toString());
	}

}
