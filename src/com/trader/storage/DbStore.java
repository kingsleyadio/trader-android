package com.trader.storage;

import java.io.IOException;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

import com.trader.office.Clerk;

public class DbStore<T> extends Store<T> {

	private static final int DB_VERSION = 1;
	private static final String DB_NAME = "com.trader.db";
	private static final String COL_ID = "id";
	private static final String COL_KEY = "key";
	private static final String COL_DATA = "data";

	private final String SQL_CREATE;
	private final String SQL_DELETE;
	private final String TABLE;

	private final SQLiteOpenHelper helper;

	DbStore(String uid, Context context, Clerk<T> clerk, Class<T> clas) {
		super(STORE_DB, uid, clerk, clas);
		TABLE = "table_" + digest(uid);
		SQL_CREATE = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
				.append(TABLE).append(" (").append(COL_ID)
				.append(" INTEGER PRIMARY KEY, ").append(COL_KEY)
				.append(" BIGINT UNIQUE, ").append(COL_DATA)
				.append(" BLOB NOT NULL)").toString();
		SQL_DELETE = new StringBuilder("DROP TABLE IF EXISTS ").append(TABLE)
				.toString();

		helper = new SQLiteOpenHelper(context, DB_NAME, null, DB_VERSION) {

			@Override
			public void onUpgrade(SQLiteDatabase db, int oldVersion,
					int newVersion) {
				db.execSQL(SQL_DELETE);
				onCreate(db);
			}

			@Override
			public void onCreate(SQLiteDatabase db) {
				db.execSQL(SQL_CREATE);
			}
		};
	}

	@Override
	protected byte[] getData(String digest) throws IOException {
		try {
			String[] intents = new String[] { COL_DATA };
			String[] selection = new String[] { digest };
			SQLiteDatabase db = helper.getReadableDatabase();
			Cursor cursor = db.query(TABLE, intents, COL_KEY + "=?", selection,
					null, null, null);
			if (cursor == null || cursor.getCount() == 0) {
				return null;
			} else {
				cursor.moveToFirst();

				return cursor.getBlob(0);
			}
		} catch (SQLiteException e) {
			Log.e("DB Store GET", "an sqlite exception occurred", e);
			throw new IOException(e.getMessage());
		}
	}

	@Override
	protected void putData(String digest, byte[] value) throws IOException {
		try {
			SQLiteDatabase db = helper.getWritableDatabase();
			ContentValues values = new ContentValues();
			values.put(COL_KEY, digest);
			values.put(COL_DATA, value);
			db.insertWithOnConflict(TABLE, null, values,
					SQLiteDatabase.CONFLICT_REPLACE);
		} catch (SQLiteException e) {
			Log.e("DB Store PUT", "an sqlite exception occurred", e);
			throw new IOException(e.getMessage());
		}
	}

	/**
	 * Removes the data referenced by this key from the store. null is simply
	 * returned to avoid the overhead of having to query the database
	 */
	@Override
	protected byte[] removeData(String digest) {
		try {
			SQLiteDatabase db = helper.getWritableDatabase();
			db.delete(TABLE, COL_KEY + "=?", new String[] { digest });
		} catch (SQLiteException e) {
			Log.e("DB Store REMOVE", "an sqlite exception occurred", e);
		}
		return null;
	}

	@Override
	protected void clearData() {
		try {
			SQLiteDatabase db = helper.getWritableDatabase();
			db.execSQL(SQL_DELETE);
			db.execSQL(SQL_CREATE);
		} catch (SQLiteException e) {
			Log.e("DB Store CLEAR", "an sqlite exception occurred", e);
		}
	}

	/**
	 * This implementation always return false because actually querying the
	 * database might add a significant overhead to the response time.
	 */
	@Override
	protected boolean contains(String digest) {
		return false;
	}

}
