package lzx.retrieval;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class ByteUtils {

	public static byte[] longToBytes(long x) {
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
		buffer.putLong(x);
		return buffer.array();
	}

	public static long bytesToLong(byte[] bytes) {
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
		buffer.put(bytes);
		buffer.flip();// need flip
		return buffer.getLong();
	}
	
	public static byte[] intToBytes(int x) {
		ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
		buffer.putInt(x);
		buffer.flip();// need flip
		return buffer.array();
	}
	
	public static int bytesToInt(byte[] bytes) {
		ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
		buffer.put(bytes);
		buffer.flip();// need flip
		return buffer.getInt();
	}
	
	public static byte boolToByte(boolean value) {
		return (byte)(value?1:0);
	}
	public static boolean byteTobool(byte by) {
		return by == 1? true: false;
	}
	
	
	public static void main(String[] args) {
		String str = "a" +"/"+"b";
		System.out.println(str);
	}
	
	
	
}
