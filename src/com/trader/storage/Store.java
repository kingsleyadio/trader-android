package com.trader.storage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import android.content.Context;
import android.util.Log;

import com.trader.office.Clerk;

public abstract class Store<T> {

	protected final Clerk<T> clerk;
	protected final String uid;
	public final Class<T> clas;
	protected final int storeType;
	protected final MemoryCache<T> cache = new MemoryCache<T>();

	Store(int storeType, String uid, Clerk<T> clerk, Class<T> clas) {
		this.uid = uid;
		this.clerk = clerk;
		this.clas = clas;
		this.storeType = storeType;
	}

	abstract byte[] get(long digest) throws IOException;

	abstract void put(long digest, byte[] value) throws IOException;

	abstract byte[] remove(long digest);

	abstract boolean containsKey(long digest);

	abstract void clearAll();

	/**
	 * Checks the resemblance between 2 stores. The kinda class managed by the
	 * clerk doesnt affect the uniqueness of a particular store
	 * 
	 * @param store
	 *            the other store to compare to
	 * @return boolean: true or false;
	 */
	public boolean similarTo(Store<T> store) {
		return similarTo(store.storeType, store.uid, store.clas);
	}

	public boolean similarTo(int storeType, String uid, Class<T> clas) {
		if (this.uid.equalsIgnoreCase(uid)) {
			if (this.storeType == storeType) {
				return true;
			}
		}
		return false;
	}

	public boolean equals(Store<T> store) {
		return (similarTo(store) && clas.equals(store.clas));
	}

	public T get(String key) {
		if (key == null) {
			throw new NullPointerException("Key cannot be null");
		}
		T object = null;
		long digest = digest(key);
		object = cache.get(digest);
		if (object != null) {
			Log.i("Store GET", "memory HIT, returning data. key = " + key);
			return object;
		} else {
			Log.i("Store GET", "memory MISS, moving to internal store");
			try {
				byte[] bytes = get(digest);
				if (bytes != null) {
					InputStream stream = new ByteArrayInputStream(bytes);
					object = clerk.inflateFromStream(stream);
					Log.i("Store GET",
							"internal store HIT, updating cache with key");
					cache.put(digest, object);
				} else {
					Log.i("Store GET", "internal store MISS, returning null");
				}
			} catch (IOException e) {
				Log.i("Store GET", "IOException encountered, returning null");
			}
			return object;
		}
	}

	public void put(String key, T value) {
		if (key == null || value == null) {
			throw new NullPointerException("Neither key nor value can be null");
		}
		long digest = digest(key);
		try {
			ByteArrayOutputStream stream = new ByteArrayOutputStream();
			clerk.deflateToStream(value, stream);
			byte[] bytes = stream.toByteArray();
			put(digest, bytes);
		} catch (IOException e) {
			Log.i("Store PUT",
					"encountered an IOException. skipping over to memory put");
		}
		cache.put(digest, value);
		Log.i("Store PUT", "new data put to store. key = " + key);
	}

	public T remove(String key) {
		if (key == null) {
			throw new NullPointerException("Key cannot be null");
		}
		long digest = digest(key);
		T value = cache.remove(digest);
		remove(digest);
		return value;
	}

	public boolean containsKey(String key) {
		if (key == null) {
			throw new NullPointerException("Key cannot be null");
		}
		long digest = digest(key);
		return (cache.containsKey(digest) || containsKey(digest));
	}

	public void clear() {
		clearAll();
		cache.clear();
		Log.i("Store CLEAR", "all data in this store has been cleared");
	}

	protected long digest(String string) {
		StringBuffer sb = new StringBuffer();
		MessageDigest digest;
		try {
			digest = MessageDigest.getInstance("MD5");
			digest.reset();
			// digest.update(string.getBytes(), 0, string.getBytes().length);
			byte[] bytes = digest.digest(string.getBytes());
			for (int i = 0; i < bytes.length; i++) {
				String s = Integer.toHexString(0xFF & bytes[i]);
				if (s.length() == 1) {
					sb.append("0").append(s);
				} else {
					sb.append(s);
				}
			}
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new BigInteger(sb.toString(), 16).longValue();
		// return Long.parseLong(sb.toString(), 16);
	}

	static class MemoryCache<T> {

		private final Map<Long, WeakReference<T>> cache = new HashMap<Long, WeakReference<T>>();

		T get(long digest) {
			// TODO Auto-generated method stub
			WeakReference<T> ref = cache.get(digest);
			return ref != null ? ref.get() : null;
		}

		void put(long digest, T value) {
			// TODO Auto-generated method stub
			WeakReference<T> ref = new WeakReference<T>(value);
			cache.put(digest, ref);
		}

		T remove(long digest) {
			// TODO Auto-generated method stub
			WeakReference<T> ref = cache.remove(digest);
			T bytes = null;
			if (ref != null) {
				bytes = ref.get();
			}
			return bytes;
		}

		public void clear() {
			// TODO Auto-generated method stub
			cache.clear();
		}

		public boolean containsKey(long digest) {
			return cache.containsKey(digest);
		}

		public int size() {
			return cache.size();
		}

		public Set<Long> keySet() {
			return cache.keySet();
		}

		public boolean isEmpty() {
			return cache.isEmpty();
		}

	}

	// //////////////////////////////////////////////////////////////////////////
	// Static entities to get the actual stores ////////////////////////////////
	// //////////////////////////////////////////////////////////////////////////
	public static final int STORE_DB = 1;
	public static final int STORE_INTERNAL_CACHE = 2;
	public static final int STORE_INTERNAL_DISK = 4;
	public static final int STORE_EXTERNAL_CACHE = 8;
	public static final int STORE_EXTERNAL_DISK = 16;

	private static final List<Store<Object>> stores = new LinkedList<Store<Object>>();

	@SuppressWarnings("unchecked")
	public static <E> Store<E> getStore(Context context, int storeType,
			String uid, Clerk<E> clerk, Class<E> clas) {
		Store<E> store = getExistingStore(storeType, uid, clerk, clas);
		if (store != null) {
			if (!store.clas.equals(clas)) {
				throw new IllegalArgumentException(
						"A store  with the same uid already exists with a different clerk");
			}
			Log.i("GET Store",
					"A store with similar signature was found. A reference to it will be returned");
		} else {
			Log.i("GET Store",
					"No existing store with similar signature was found, attempting to create a new store");
			if (storeType == STORE_DB) {
				store = new DbStore<E>(uid, context, clerk, clas);
			} else {
				store = new FileStore<E>(storeType, uid, context, clerk, clas);
			}
			stores.add((Store<Object>) store);
		}
		return store;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static <E> Store<E> getExistingStore(int storeType, String uid,
			Clerk<E> clerk, Class clas) {
		for (Store<Object> s : stores) {
			if (s.similarTo(storeType, uid, clas)) {
				return (Store<E>) s;
			}
		}
		return null;
	}

}
