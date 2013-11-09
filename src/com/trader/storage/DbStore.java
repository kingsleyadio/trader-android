package com.trader.storage;

import java.io.IOException;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
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
		// TODO Auto-generated constructor stub
		TABLE = "table_" + digest(uid);
		SQL_CREATE = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
				.append(TABLE).append(" (").append(COL_ID)
				.append(" INTEGER PRIMARY KEY, ").append(COL_KEY)
				.append(" BIGINT UNIQUE, ").append(COL_DATA)
				.append(" BLOB NOT NULL)").toString();
		SQL_DELETE = new StringBuilder("DROP TABLE IF EXISTS ")
				.append(TABLE).toString();
		Log.i("DB CREATE", SQL_CREATE);
		Log.i("DB DELETE", SQL_DELETE);

		helper = new SQLiteOpenHelper(context, DB_NAME, null, DB_VERSION) {

			@Override
			public void onUpgrade(SQLiteDatabase db, int oldVersion,
					int newVersion) {
				// TODO Auto-generated method stub
				db.execSQL(SQL_DELETE);
				onCreate(db);
			}

			@Override
			public void onCreate(SQLiteDatabase db) {
				// TODO Auto-generated method stub
				db.execSQL(SQL_CREATE);
			}
		};
	}

	@Override
	byte[] get(long digest) throws IOException {
		// TODO Auto-generated method stub
		String[] intents = new String[] {COL_DATA};
		String[] selection = new String[]{Long.toHexString(digest)};
		SQLiteDatabase db = helper.getReadableDatabase();
		Cursor cursor = db.query(TABLE, intents, COL_KEY + "=?", selection, null, null, null);
        if (cursor == null || cursor.getCount() == 0) {
            return null;
        } else {
            cursor.moveToFirst();

            return cursor.getBlob(0);
        }
	}

	@Override
	void put(long digest, byte[] value) throws IOException {
		// TODO Auto-generated method stub
		SQLiteDatabase db = helper.getWritableDatabase();
		ContentValues values = new ContentValues();
		values.put(COL_KEY, digest);
		values.put(COL_DATA, value);
		db.insertWithOnConflict(TABLE, null, values, SQLiteDatabase.CONFLICT_REPLACE);
	}

	/**
	 * Removes the data referenced by this key from the store.
	 * null is simply returned to avoid the overhead of having to query the database
	 */
	@Override
	byte[] remove(long digest) {
		// TODO Auto-generated method stub
		SQLiteDatabase db = helper.getWritableDatabase();
		db.delete(TABLE, COL_KEY + "=?", new String[]{Long.toHexString(digest)});
		return null;
	}

	@Override
	void clearAll() {
		// TODO Auto-generated method stub
		SQLiteDatabase db = helper.getWritableDatabase();
		db.execSQL(SQL_DELETE);
		db.execSQL(SQL_CREATE);
	}

	/**
	 * This implementation always return false because actually querying the 
	 * database might add a significant overhead to the response time.
	 */
	@Override
	boolean containsKey(long digest) {
		// TODO Auto-generated method stub
		return false;
	}

}
