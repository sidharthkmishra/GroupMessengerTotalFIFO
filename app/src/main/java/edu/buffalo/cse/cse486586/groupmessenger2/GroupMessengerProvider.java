package edu.buffalo.cse.cse486586.groupmessenger2;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.util.Log;

/**
 * GroupMessengerProvider is a key-value table. Once again, please note that we do not implement
 * full support for SQL as a usual ContentProvider does. We re-purpose ContentProvider's interface
 * to use it as a key-value table.
 * 
 * Please read:
 * 
 * http://developer.android.com/guide/topics/providers/content-providers.html
 * http://developer.android.com/reference/android/content/ContentProvider.html
 * 
 * before you start to get yourself familiarized with ContentProvider.
 * 
 * There are two methods you need to implement---insert() and query(). Others are optional and
 * will not be tested.
 * 
 * @author stevko
 *
 */
public class GroupMessengerProvider extends ContentProvider {

    //DB File
    private static final String DATABASE_NAME = "messageStore.db";

    //Wed on't need the version for this. But just keeping 1 for future like PA2 B (Not Sure)
    private static final int VERSION = 1;
    private SQLiteDatabase mDatabase;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // You do not need to implement this.
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        /*
         * TODO: You need to implement this method. Note that values will have two columns (a key
         * column and a value column) and one row that contains the actual (key, value) pair to be
         * inserted.
         *
         * For actual storage, you can use any option. If you know how to use SQL, then you can use
         * SQLite. But this is not a requirement. You can use other storage options, such as the
         * internal storage option that we used in PA1. If you want to use that option, please
         * take a look at the code for PA1.
         */
        boolean isKeyPresent = false;
        //If key exists at DB then need to update or else insert
        String key = (String) values.get(GroupMessengerProvider.MessengerDbSchema.MessageTable.Cols.KEY);

        Cursor resultCursor = query(uri, null, key, null, null);
        if (resultCursor != null) {
            if (resultCursor.moveToFirst()){
                do{
                    String returnKey = resultCursor.getString(resultCursor.getColumnIndex(MessengerDbSchema.MessageTable.Cols.KEY));
                    if (returnKey.equals(key)) {
                        isKeyPresent = true;
                        break;
                    }
                }while(resultCursor.moveToNext());
            }
            resultCursor.close();
        }
        if(isKeyPresent == true) {
            update(uri, values, key, null);
        } else {
            mDatabase.insert(MessengerDbSchema.MessageTable.NAME, null, values);
            Log.v("insert", values.toString());
        }

        return uri;
    }

    @Override
    public boolean onCreate() {
        // If you need to perform any one-time initialization task, please do it here.
        Context context = getContext();
        mDatabase = new MessengerDBHelper(getContext()).getWritableDatabase();
        return (mDatabase == null)? false : true;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        mDatabase.update(MessengerDbSchema.MessageTable.NAME, values,
                MessengerDbSchema.MessageTable.Cols.KEY + " = ?",
                new String[] { selection });
        return 0;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        /*
         * TODO: You need to implement this method. Note that you need to return a Cursor object
         * with the right format. If the formatting is not correct, then it is not going to work.
         *
         * If you use SQLite, whatever is returned from SQLite is a Cursor object. However, you
         * still need to be careful because the formatting might still be incorrect.
         *
         * If you use a file storage option, then it is your job to build a Cursor * object. I
         * recommend building a MatrixCursor described at:
         * http://developer.android.com/reference/android/database/MatrixCursor.html
         */
        String overrideSelection = null;
        String [] overrideSelectionArg = null;
        if (selection != null) {
            overrideSelection = MessengerDbSchema.MessageTable.Cols.KEY + "= ?";
            overrideSelectionArg = new String[] { selection };
        }

        Cursor cursor = mDatabase.query(
                MessengerDbSchema.MessageTable.NAME,
                projection, // columns - null selects all columns
                overrideSelection,
                overrideSelectionArg,
                null, // groupBy
                null, // having
                sortOrder
        );

        return cursor;
    }

    //This part of code is inspired by book:
    //Android Programming: The Big Nerd Ranch Guide, Third Edition
    //Android provides the SQLiteOpenHelper class to check if the db is already exists,
    //if it doesn't it creates and create the initial db etc.
    //We need have a derived class from the abstract class
    public class MessengerDBHelper extends SQLiteOpenHelper {
        private static final int VERSION = 1;
        private static final String DATABASE_NAME = "crimeBase.db";

        public MessengerDBHelper(Context context) {
            super(context, DATABASE_NAME, null, VERSION);
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
            db.execSQL("create table " + MessengerDbSchema.MessageTable.NAME + "(" +
                    MessengerDbSchema.MessageTable.Cols.KEY + ", " +
                    MessengerDbSchema.MessageTable.Cols.VALUE +
                    ")"
            );
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

        }
    }

    class MessengerDbSchema {
        final class MessageTable {
            static final String NAME = "message";

            final class Cols {
                static final String KEY = "key";
                static final String VALUE = "value";
            }
        }
    }
}
