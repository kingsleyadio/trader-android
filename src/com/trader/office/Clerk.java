package com.trader.office;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface Clerk<T> {

	public void deflateToStream(T object, OutputStream stream) throws IOException;
	
	public T inflateFromStream(InputStream stream) throws IOException;
}
