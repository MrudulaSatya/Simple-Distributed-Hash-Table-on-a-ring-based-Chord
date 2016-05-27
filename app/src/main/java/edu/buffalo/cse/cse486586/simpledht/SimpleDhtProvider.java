package edu.buffalo.cse.cse486586.simpledht;

import java.io.NotSerializableException;
import java.io.OutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.CursorIndexOutOfBoundsException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.graphics.Matrix;
import android.net.Uri;
import android.content.Context;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import android.database.MatrixCursor;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleDhtProvider extends ContentProvider {
    private static final String TAG = SimpleDhtProvider.class.getSimpleName();

    private static final int SERVER_PORT = 10000;
    private static final String masterNode = "11108";

    private static final String[] DBCOL = {"key", "value"};

    private static ConcurrentHashMap<String, String> Localstorage = new ConcurrentHashMap<String, String>();

    public static boolean wait = true;
    private String[] selfNodeID, succNodeID, preNodeID;

    private final packet globalPacketStack = new packet();

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        packet pack;
        boolean STARTNODE = (uri != null);
        if (selection.equals("@") || selection.equals("*")) {
            Localstorage.clear();
            if (selection.equals("*") && succNodeID[1].equals(selectionArgs[0]) == false) { // check next node is not the starting node
                pack = new packet(succNodeID[1], selection, null, selectionArgs[0], null, QueryNature.DELETE, null);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, pack);
            }
        } else if (isPacktForCurrNode(selection)) {
            Localstorage.remove(selection);
        } else if(!STARTNODE){
            pack = new packet(succNodeID[1], selection, null, selectionArgs[0], null, QueryNature.DELETE, null);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, pack);
        }
        else
        {
            pack = new packet(succNodeID[1], selection, null, selfNodeID[1], null, QueryNature.DELETE, null);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, pack);
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        try {
            String key = values.getAsString(DBCOL[0]);
            String value = values.getAsString(DBCOL[1]);
            if (isPacktForCurrNode(key)) {
                Log.e("trying to put", key + value);
                Localstorage.put(key, value);
            } else {
                packet pack = new packet(succNodeID[1], key, value, null, null, QueryNature.INSERT, null);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, pack);
            }
        }catch(Exception e)
        {
            Log.e("Exception caught in inserting content",e.toString());
        }
        return uri;
    }

    @Override
    public boolean onCreate() {

/*        CONTENT_URI = Uri.parse("content://edu.buffalo.cse.cse486586.groupmessenger2.provider" + "/" + mydb.TABLE_NAME);
        mydb = new MyDB(getContext());
        db = mydb.getWritableDatabase();

  */      TelephonyManager tel = (TelephonyManager)this.getContext().getSystemService(
                Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        selfNodeID = new String[]{portStr,Integer.toString(Integer.parseInt(portStr) * 2)};

        Log.e("creating something",selfNodeID[0]+selfNodeID[1]);
        preNodeID = succNodeID = new String[]{"",""};
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }
        Log.e("I should have entered here",selfNodeID[1]);

        if (!selfNodeID[1].equals(masterNode))
        {
            Log.e("going to join the network",selfNodeID[1]);
            packet p = new packet(masterNode, selfNodeID, QueryNature.NEWJOIN_REQUEST);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, p);

        }

        return true;
    }

    public Cursor query(packet pckt)
    {
        Log.e("querying","here");
        MatrixCursor cursor = new MatrixCursor(DBCOL);
        packet pack;
        Log.e("going to call","before switch");
        String key = pckt.key;
        String initNode = pckt.initNode;
        switch (key.charAt(0)) {
            case '@':
                for(Map.Entry<String, String> mpIter : Localstorage.entrySet())
                    cursor.addRow(new String[]{mpIter.getKey(),mpIter.getValue()});
                break;
            case '*':
                if (succNodeID[0].length() == 0) {
                    for(Map.Entry<String, String> mpIter : Localstorage.entrySet())
                        cursor.addRow(new String[]{mpIter.getKey(),mpIter.getValue()});
                    break;
                }
                pckt.DBcursor.putAll(Localstorage);
                pack = new packet(succNodeID[1], pckt);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, pack);

                if (initNode.equals(selfNodeID[1])){//uri != null) {
                    Log.e("inside not null", "ab chain start");
                    synchronizer();
                    for(Map.Entry<String, String> mpIter : globalPacketStack.DBcursor.entrySet())
                        cursor.addRow(new String[]{mpIter.getKey(),mpIter.getValue()});
                }
                break;
            default:
                Log.e("inside default","to query");
                if (isPacktForCurrNode(key)) {
                    Log.e("inside default","inside if");
                    boolean checkempty = (initNode.length()!=0);
                    boolean trueorfalse = (checkempty&&(!initNode.equals(selfNodeID[1])));
                    if (trueorfalse) {
                        Log.e("inside default", "inside ori check");
                        pack = new packet(initNode,key, Localstorage.get(key),pckt);
                        //pack = new packet(initNode, key, Localstorage.get(key), initNode, initNode, QueryNature.QUERY, null);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, pack);
                    } else {
                        cursor.addRow(new String[]{key, Localstorage.get(key)});
                        Log.e("values in Localstorage", Localstorage.get(key));
                    }
                    return cursor;
                } else {
                    Log.e("values stored", succNodeID[1] + " " + key);

                    pack = new packet(succNodeID[1], pckt);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, pack);
                    if (initNode.equals(selfNodeID[1])){//uri != null) {
                        synchronizer();
                        cursor.addRow(new String[]{key, globalPacketStack.value});
                    }
                }
        }
        return cursor;
    }

    public void synchronizer()
    {
        wait = true;
        while(wait);
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {

        packet pckt = new packet(selection,selfNodeID[1], QueryNature.QUERY);

        Cursor cursor = query( pckt);
//        Log.e("values in cursor", cursor);
        return cursor;
    }
    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private void newJoiny(packet pack) {
        packet newPack;
        if (isPacktForCurrNode(pack.key))
        {
            if (preNodeID[0].length() == 0)
            {
                Log.e("in if condition of joiny","wooo");
                preNodeID = new String[]{pack.key, pack.preNodePortAdd};
                succNodeID = new String[]{pack.key, pack.preNodePortAdd};
                newPack = new packet(preNodeID[1], selfNodeID, QueryNature.NEWJOIN_REPLY);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, newPack);
            } else
            {
                Log.e("in else condition of joiny","ab sahi hai ");
                Log.e("values in packets",preNodeID[1]+" "+pack.preNodePortAdd);
                newPack = new packet(preNodeID[1], pack.key, null, null, pack.preNodePortAdd, QueryNature.NEWJOIN_SUCC,null);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, newPack);
                newPack = new packet(pack.preNodePortAdd, preNodeID[0], selfNodeID[0], preNodeID[1], selfNodeID[1], QueryNature.NEWJOIN_REPLY,null);
                Log.e("clear first connection , set succ and pred",preNodeID[1]+" "+pack.preNodePortAdd);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, newPack);
                preNodeID = new String[]{pack.key, pack.preNodePortAdd};
            }
        } else
        {
            Log.e("in else of newjoiny",pack.key);
            newPack = new packet(succNodeID[1],pack);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, newPack);
        }
    }



    private String findNodeID(String key)
    {
        try
        {
            return genHash(key);
        } catch (NoSuchAlgorithmException e)
        {
            Log.e(TAG, "Hash Exception:" + e.getMessage());
        }
        return "";
    }


    private boolean isPacktForCurrNode(String key)
    {

        if (preNodeID[0].length() == 0 )
        {
            Log.e("preNodeID is empty","so add"+key);
            return true;
        }
        String pckID = findNodeID(key);
        String thisNode = findNodeID(selfNodeID[0]);
        String preNode = findNodeID(preNodeID[0]);
        Log.e("see values in ID's",preNodeID[1]+" "+succNodeID[1]+" "+selfNodeID[1]);

        Log.e("comparisons",preNode.compareTo(thisNode)+" "+pckID.compareTo(preNode)+" "+pckID.compareTo(thisNode));
        /******************** If this is first node, check if the previous node has the biggest value in chord **********************/
        if ((preNode.compareTo(thisNode) > 0) &&
            ((pckID.compareTo(preNode) > 0 && pckID.compareTo(thisNode) > 0) ||
            (preNode.compareTo(pckID) > 0 && thisNode.compareTo(pckID) >= 0)))
        {
            return true;
        }
        else if (pckID.compareTo(preNode) >= 0 && thisNode.compareTo(pckID) >=0 )
        {
            return true;
        }
        Log.e("didnt send true",key);

        return false;
        //return true;
    }


        private class ServerTask extends AsyncTask<ServerSocket, String, Void>{

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try {
                while(true) {
                    Socket socket = serverSocket.accept();
                    ObjectInputStream packetStream = new ObjectInputStream(socket.getInputStream());
                    packet pack = (packet)packetStream.readObject();
                    switch (pack.QueryNature) {
                        case NEWJOIN_REQUEST:
                            Log.e("I got the join request",selfNodeID[1]);
                            Log.e("asking for join is",pack.preNodePortAdd+" "+pack.succNodePrtAdd+" "+pack.key+" "+pack.value);
                            newJoiny(pack);
                            break;
                        case NEWJOIN_REPLY:
                            Log.e("got the join reply", pack.preNodePortAdd);
                            preNodeID = new String[]{pack.key,pack.preNodePortAdd};
                            succNodeID = new String[]{pack.value, pack.succNodePrtAdd};
                            break;
                        case NEWJOIN_SUCC:
                            succNodeID = new String[]{pack.key, pack.succNodePrtAdd};
                            break;
                        case INSERT:
                            ContentValues cv = new ContentValues();
                            cv.put(DBCOL[0], pack.key);
                            cv.put(DBCOL[1], pack.value);
                            insert(null, cv);
                            break;
                        case QUERY:
                            Log.e("values in packet",pack.key+" "+pack.preNodePortAdd);
                            if (selfNodeID[1].equals(pack.initNode)) {
                                globalPacketStack.assign(pack);
                                wait = false;

                                break;
                            }
                            query(pack);
                            break;
                        case DELETE:
                            delete(null, pack.key, new String[]{pack.initNode});
                            break;
                        default:
                            Log.e(TAG, "QueryNature Found: " + pack.QueryNature);
                    }
                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "ServerTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ServerTask socket IOException:\n" + e.getMessage());
            } catch (ClassNotFoundException e) {
                Log.e(TAG, "ServerTask ObjectInputStream ClassNotFoundException");
            }

            return null;
        }

    }

    /***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
     * an enter key press event.
     *
     * @author stevko
     *
     */
    private class ClientTask extends AsyncTask<packet, Void, Void> {

        @Override
        protected Void doInBackground(packet... packobj) {
            packet p = packobj[0];
            try {
                Log.e("I am going to create exception", p.destiny+" lets see the flag "+(p.QueryNature));
                Log.e("key in pack",p.key);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(p.destiny));
                OutputStream s = socket.getOutputStream();
                ObjectOutputStream obj = new ObjectOutputStream(s);
                obj.writeObject(p);
                obj.close();
                socket.close();
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                e.printStackTrace();
                Log.e(TAG, "ClientTask socket IOException"+e.getCause());
            }

            return null;
        }
    }
/*    private class MyDB extends SQLiteOpenHelper {

        public static final String DATABASE_NAME = "USERS";
        public static final int DATABASE_VERSION = 2;
        public static final String TABLE_NAME = "UserKeyValue";
        public static final String TABLE_CREATE =
                "CREATE TABLE " + TABLE_NAME +
                        " ( key TEXT PRIMARY KEY , value TEXT REPLACE);";

        MyDB(Context context) {
            super(context, DATABASE_NAME, null, DATABASE_VERSION);
        }


        @Override
        public void onUpgrade(SQLiteDatabase arg0, int arg1, int arg2) {
            onCreate(db);
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
            db.execSQL(TABLE_CREATE);}
    }*/
}
class packet implements Serializable
{
    private static final long serialVersionUID = 789456345456678789L;

    public String destiny;
    public String key;
    public String value;
    public String preNodePortAdd;
    public String succNodePrtAdd;
    public String initNode;
    public QueryNature QueryNature;
    public ConcurrentHashMap<String, String> DBcursor;

    public packet()
    {
        destiny = "";
        key = "";
        value = "";
        preNodePortAdd = "";
        succNodePrtAdd = "";
        initNode = "";
        DBcursor = new ConcurrentHashMap<String, String>();
    }
    public packet(packet pckt)
    {
        assign(pckt);
    }
    public packet(String destiny,String key, String value, packet pckt)
    {
        this(pckt);
        this.destiny = destiny;
//        this.initNode = pckt.initNode;
  //      this.preNodePortAdd = pckt.preNodePortAdd;
    //    this.succNodePrtAdd = pckt.succNodePrtAdd;
      //  this.DBcursor = pckt.DBcursor;
        this.key = key;
        this.value = value;
        //this.QueryNature = pckt.QueryNature;

    }
    public packet(String key, String initNode, QueryNature nature)
    {
        this();
        this.key = key;
        this.initNode = initNode;
        this.QueryNature = nature;
    }
    public packet(String destiny, packet pack)
    {
        this(pack);
        this.destiny = destiny;
        Log.e("see what I got destiny",destiny);
//        this.key = pack.key;
  //      this.value = pack.value;
    //    this.preNodePortAdd = pack.preNodePortAdd;
      //  this.succNodePrtAdd = pack.succNodePrtAdd;
        //this.QueryNature = pack.QueryNature;
        //this.DBcursor = pack.DBcursor;
        //this.initNode = pack.initNode;
    }
    public packet(String destiny, String[] Node, QueryNature nature)
    {
        this();
        this.destiny = destiny;
        key = Node[0];
        value = Node[0];
        preNodePortAdd = Node[1];
        succNodePrtAdd = Node[1];
        QueryNature = nature;
        initNode = Node[1];
    }
    public packet(String destPortAdd, String key, String value, String preNodePortAdd, String succNodePrtAdd, QueryNature QueryNature, ConcurrentHashMap<String, String> DBcursor)
    {
        this();
        this.destiny = destPortAdd;
        this.key = key;
        this.value = value;
        this.preNodePortAdd = preNodePortAdd;
        this.succNodePrtAdd = succNodePrtAdd;
        this.QueryNature = QueryNature;
        this.initNode = preNodePortAdd;
    }
    public void assign(packet pckt)
    {
        this.destiny = pckt.destiny;
        this.preNodePortAdd = pckt.preNodePortAdd;
        this.succNodePrtAdd = pckt.succNodePrtAdd;
        this.key = pckt.key;
        this.value = pckt.value;
        this.QueryNature = pckt.QueryNature;
        this.DBcursor = pckt.DBcursor;
        this.initNode = pckt.initNode;
    }
}
 enum QueryNature { NEWJOIN_REQUEST, NEWJOIN_REPLY, NEWJOIN_SUCC, INSERT, QUERY, DELETE }
