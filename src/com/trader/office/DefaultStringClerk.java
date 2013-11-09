package com.trader.office;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class DefaultStringClerk implements Clerk<String> {

	@Override
	public void deflateToStream(String object, OutputStream stream)
			throws IOException {
		// TODO Auto-generated method stub
		stream.write(object.getBytes());
	}

	@Override
	public String inflateFromStream(InputStream stream) throws IOException {
		// TODO Auto-generated method stub
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		byte[] bytes = new byte[1024];
		int l = 0;
		while ((l = stream.read(bytes)) > -1) {
			os.write(bytes, 0, l);
		}
		return new String(os.toByteArray(), "utf-8");
	}

}
