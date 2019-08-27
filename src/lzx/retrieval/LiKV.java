package lzx.retrieval;

import java.io.IOException;

public class LiKV implements KVstore{
	
	
	public BufferedReader br;
	
	public LiKV(String name) throws IOException {
		br = new BufferedReader(name);
	}

	@Override
	public byte[] read(byte[] key)  {
		// TODO Auto-generated method stub
		KVPair kv;
		try {
			kv = br.get(key);
			if(kv != null)
				return kv.elem2;
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public static void main(String[] args) throws IOException  {
		LiKV kv = new LiKV("/home/lzx/eclipse-workspace/BigFileRetrieval/tests/a.data");
		byte[] key = {'p'};
		byte[] res = kv.read(key);
		if(res != null)
			System.out.println(new String(res));
		else
			System.out.println("null");
		System.out.println(kv.br.toString());
		byte[] key2 = {'w'};
		res = kv.read(key2);
		if(res != null)
			System.out.println(new String(res));
		else
			System.out.println("null");
	}

}
