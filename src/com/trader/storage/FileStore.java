package com.trader.storage;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import android.content.Context;

import com.trader.office.Clerk;

public class FileStore<T> extends Store<T> {

	private static final String TRADER = "com.trader";
	private final File base;

	FileStore(int storeType, String uid, Context context, Clerk<T> clerk,
			Class<T> clas) {
		super(storeType, uid, clerk, clas);
		// TODO Auto-generated constructor stub
		File dir = null;
		switch (storeType) {
		case STORE_INTERNAL_CACHE:
			dir = context.getCacheDir();
			break;
		case STORE_INTERNAL_DISK:
			dir = context.getFilesDir();
			break;
		case STORE_EXTERNAL_CACHE:
			dir = context.getExternalCacheDir();
			break;
		case STORE_EXTERNAL_DISK:
			dir = context.getExternalFilesDir(null);
			break;
		default:
			throw new IllegalArgumentException(
					"An invalid storeType was specified" + storeType);
		}
		if (dir == null) {
			throw new NullPointerException("External storage is not mounted");
		}
		base = new File(dir, TRADER);
		if (!base.exists()) {
			base.mkdirs();
		}
	}

	@Override
	byte[] get(long digest) throws IOException {
		// TODO Auto-generated method stub
		File file = new File(base, Long.toHexString(digest));
		if (!file.exists()) {
			return null;
		}
		InputStream is = null;
		try {
			is = new FileInputStream(file);
			ByteArrayOutputStream bs = new ByteArrayOutputStream();
			byte[] bytes = new byte[1024];
			int l = 0;
			while ((l = is.read(bytes)) > -1) {
				bs.write(bytes, 0, l);
			}
			return bs.toByteArray();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (is != null) {
				is.close();
			}
		}
		return null;
	}

	@Override
	void put(long digest, byte[] value) throws IOException {
		// TODO Auto-generated method stub
		if (!base.exists()) {
			base.mkdirs();
		}
		File file = new File(base, Long.toHexString(digest));
		OutputStream os = null;
		try {
			os = new FileOutputStream(file);
			os.write(value);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (os != null) {
				os.close();
			}
		}
	}

	/**
	 * Removes the data referenced by this key from the store.
	 * null is simply returned to avoid the overhead of having to read the file
	 * before returning.
	 */
	@Override
	byte[] remove(long digest) {
		// TODO Auto-generated method stub
		File file = new File(base, Long.toHexString(digest));
		file.delete();
		return null;
	}

	@Override
	void clearAll() {
		// TODO Auto-generated method stub
		File[] files = base.listFiles();
		if (files == null)
			return;
		for (File f : files)
			f.delete();
		// base.delete();
	}

	public boolean similarTo(FileStore<T> store) {
		// TODO Auto-generated method stub
		if (base != store.base) {
			return false;
		}
		return super.similarTo(store);
	}

	@Override
	boolean containsKey(long digest) {
		// TODO Auto-generated method stub
		File file = new File(base, Long.toString(digest, 16));
		return file.exists();
	}

}
