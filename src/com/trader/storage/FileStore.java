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
import android.util.Log;

import com.trader.office.Clerk;

public class FileStore<T> extends Store<T> {

	private static final String TRADER = "com.trader";
	private final File base;

	FileStore(int storeType, String uid, Context context, Clerk<T> clerk,
			Class<T> clas) {
		super(storeType, uid, clerk, clas);
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
			throw new NullPointerException("External storage is not available");
		}
		base = new File(dir, TRADER);
		if (!base.exists()) {
			base.mkdirs();
		}
	}

	@Override
	protected
	byte[] getData(String digest) throws IOException {
		File file = new File(base, digest);
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
			Log.e("FileStore GET", "file not found. the cache probably never existed");
		} finally {
			if (is != null) {
				is.close();
			}
		}
		return null;
	}

	@Override
	protected
	void putData(String digest, byte[] value) throws IOException {
		if (!base.exists()) {
			base.mkdirs();
		}
		File file = new File(base, digest);
		OutputStream os = null;
		try {
			os = new FileOutputStream(file);
			os.write(value);
		} catch (FileNotFoundException e) {
			Log.e("FileStore PUT", "unable to open file for writing");
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
	protected
	byte[] removeData(String digest) {
		File file = new File(base, digest);
		file.delete();
		return null;
	}

	@Override
	protected
	void clearData() {
		File[] files = base.listFiles();
		if (files == null)
			return;
		for (File f : files)
			f.delete();
		// base.delete();
	}

	public boolean similarTo(FileStore<T> store) {
		if (base != store.base) {
			return false;
		}
		return super.similarTo(store);
	}

	@Override
	protected
	boolean contains(String digest) {
		File file = new File(base, digest);
		return file.exists();
	}

}
