package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.util.Pair;
import android.view.KeyEvent;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {

    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String PORT_LIST[] = {"11108", "11112", "11116", "11120", "11124"};
    static final int SERVER_PORT = 10000;
    static final int NUMBER_OF_AVDS = 5;

    static int myProcessNumber = 0;

    static Queue<Pair<Integer,String>> mSendQueue = new ConcurrentLinkedQueue<Pair<Integer,String>>();
    static ConcurrentHashMap<String, Integer> mCountProposedSeqMap = new ConcurrentHashMap<String, Integer>();

    private Button mSendButton;
    private EditText mEditText;
    private TextView mShowMsg;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        mShowMsg = (TextView) findViewById(R.id.textView1);
        mShowMsg.setMovementMethod(new ScrollingMovementMethod());

        //Fetch the messages from the Content Provider if any
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority("edu.buffalo.cse.cse486586.groupmessenger2.provider");
        uriBuilder.scheme("content");
        Cursor resultCursor = getContentResolver().query(uriBuilder.build(), null, null, null, null);
        if (resultCursor != null) {
            if (resultCursor.moveToFirst()){
                do{
                    String returnKey = resultCursor.getString(resultCursor.getColumnIndex(GroupMessengerProvider.MessengerDbSchema.MessageTable.Cols.KEY));
                    String returnValue = resultCursor.getString(resultCursor.getColumnIndex(GroupMessengerProvider.MessengerDbSchema.MessageTable.Cols.VALUE));
                    mShowMsg.append(returnKey + " : " + returnValue + "\n");
                }while(resultCursor.moveToNext());
            }
            resultCursor.close();
        }

        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(mShowMsg, getContentResolver()));

        /* This Code is taken from PA1
         * Calculate the port number that this AVD listens on.
         * It is just a hack that I came up with to get around the networking limitations of AVDs.
         * The explanation is provided in the PA1 spec.
         */
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        myProcessNumber = ((Integer.parseInt(portStr)-5554)/2) + 1;
        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
        try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             */
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            serverSocket.setSoTimeout(300);

            new ReceiveEventTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            new SendEventTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, myProcessNumber);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }
        //Retrieve a pointer to the Send Button (button4) defined in the layout
        mSendButton = (Button) findViewById(R.id.button4);
        //Retrieve a pointer to the input box (EditText) defined in the layout
        mEditText = (EditText) findViewById(R.id.editText1);

        //Setting Listener for the Send Button which will send the msg to all other AVDs
        mSendButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String msg = mEditText.getText().toString();
                if(!msg.isEmpty()) {
                    msg += "\n";
                    //Resent the input box area
                    mEditText.setText("");
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
                }
                //mShowMsg.append(msg);
            }
        });

        //In case of enter key press also perform multi cast
        mEditText.setOnKeyListener(new View.OnKeyListener() {
            @Override
            public boolean onKey(View v, int keyCode, KeyEvent event) {
                if ((event.getAction() == KeyEvent.ACTION_DOWN) &&
                        (keyCode == KeyEvent.KEYCODE_ENTER)) {
                    String msg = mEditText.getText().toString();
                    if(!msg.isEmpty()) {
                        msg += "\n";
                        //Resent the input box area
                        mEditText.setText("");
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
                        return true;
                    }
                }
                return false;
            }
        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    /***
     * Most of the below part code is been taken from PA1
     * ReceiveEventTask is an AsyncTask that should handle incoming events.
     *
     * Please make sure you understand how AsyncTask works by reading
     * http://developer.android.com/reference/android/os/AsyncTask.html
     */
    private class ReceiveEventTask extends AsyncTask<ServerSocket, String, Void> {
        class ISISMsgBuffer implements Comparable<ISISMsgBuffer> {
            private Float mSeqNumber;
            private String mMsg;
            private boolean mIsDeliverable;
            private Integer mOriginationAVD;

            public ISISMsgBuffer(Float seqNum, String msg, Integer originationAVD) {
                this.mSeqNumber = seqNum;
                this.mMsg = msg;
                this.mOriginationAVD = originationAVD;
                this.mIsDeliverable = false;
            }

            @Override
            public int compareTo(ISISMsgBuffer other) {
                return this.mSeqNumber.compareTo(other.getSeqNumber());
            }

            public  Integer getOriginationAVD() {
                return mOriginationAVD;
            }

            public Float getSeqNumber() {
                return mSeqNumber;
            }

            public String getMsg() {
                return mMsg;
            }

            public boolean getIsDeliverable() {
                return mIsDeliverable;
            }

            public void setIsDeliverable(boolean isDeliverable) {
                this.mIsDeliverable = isDeliverable;
            }

            public void setSeqNumber(Float mSeqNumber) {
                this.mSeqNumber = mSeqNumber;
            }

            @Override
            public boolean equals(Object obj) {
                if (obj != null && getClass() == obj.getClass()) {
                    ISISMsgBuffer other = (ISISMsgBuffer)obj;
                    return (this.getMsg().equals(other.getMsg()));
                }
                return false;
            }
        }

        private int mSeqNum = 0;
        private Integer mSeqNumForDb = 0;
        private Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            HashMap<Integer, BufferedReader> receiveSocketReaderMap = new HashMap<Integer, BufferedReader>();
            PriorityQueue<ISISMsgBuffer> ISISPriorQueue = new PriorityQueue<ISISMsgBuffer>();

            //Fetch the sequence number from DB for first time
            try {
                Cursor resultCursor = getApplicationContext().getContentResolver().query(mUri, null, null, null, null);
                if (resultCursor != null) {
                    if (resultCursor.moveToLast()){
                        String returnSeqNum = resultCursor.getString(resultCursor.getColumnIndex(GroupMessengerProvider.MessengerDbSchema.MessageTable.Cols.KEY));
                        mSeqNum = (int) Float.parseFloat(returnSeqNum);
                    }
                    resultCursor.close();
                }
            } catch (Exception e) {
                Log.e(TAG, e.toString());
            }

            //Take one map for heartbeat msgs
            HashMap<Integer, Long> heartbeatMap = new HashMap<Integer, Long>();
            for(int nIndex = 1; nIndex <= NUMBER_OF_AVDS; nIndex++)
                heartbeatMap.put(nIndex, System.currentTimeMillis());

            boolean isConnectionSetupNotCompleted = true;
            boolean isFirstDelay = true;
            while(true) {
                if(isConnectionSetupNotCompleted) {
                    try {
                        //When a client connects to server, a client socket will be returned
                        Socket clientSocket = serverSocket.accept();
                        clientSocket.setSoTimeout(500);

                        //Create a socket reader to get the msg from the client socket
                        BufferedReader clientSocketReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                        String proc = clientSocketReader.readLine();
                        Log.e(TAG, "step 1: " + proc);
                        //Store the socket into the map with corresponding avd number
                        receiveSocketReaderMap.put(Integer.parseInt(proc), clientSocketReader);
                        heartbeatMap.put(Integer.parseInt(proc), System.currentTimeMillis());
                        if(receiveSocketReaderMap.size() == NUMBER_OF_AVDS) {
                            isConnectionSetupNotCompleted = false;
                        }
                    } catch (SocketTimeoutException st) {
                        //do nothing
                    } catch (IOException e) {
                        e.printStackTrace();
                        Log.e(TAG, e.getMessage(), e);
                    }
                }

                long avgDelayTime = 0;
                long maxDelay = 0;
                int maxDelayedAVD = 0;
                for (Map.Entry<Integer, Long> heartBeatEntry : heartbeatMap.entrySet()) {
                    //Check the heartbeat time for other AVDs

                    if (heartBeatEntry.getKey() != myProcessNumber) {
                        Long startTime = heartBeatEntry.getValue();
                        Long stopTime = System.currentTimeMillis();
                        Long elapsedTime = stopTime - startTime;
                        avgDelayTime += elapsedTime;
                        if (maxDelay < elapsedTime) {
                            maxDelay = elapsedTime;
                            maxDelayedAVD = heartBeatEntry.getKey();
                        }
                    }
                }

                avgDelayTime = avgDelayTime/(heartbeatMap.size()-1);

                if (maxDelay-avgDelayTime > 5000l) {
                    if (isConnectionSetupNotCompleted && isFirstDelay) {
                        isFirstDelay = false;
                        heartbeatMap.put(maxDelayedAVD, System.currentTimeMillis());
                    } else {
                        Log.e(TAG, "Total info: maxDelay: " + maxDelay + " and avgDelayTime: "
                                + avgDelayTime + " for AVD: " + maxDelayedAVD);
                        Log.e(TAG, "Removing entry for AVD: " + maxDelayedAVD);
                        isConnectionSetupNotCompleted = false;
                        receiveSocketReaderMap.remove(maxDelayedAVD);
                        //Remove all the entries related to failure node and mark ProposedSeqMap accordingly
                        for (Object object : ISISPriorQueue) {
                            ISISMsgBuffer msgAtHoldQueue = (ISISMsgBuffer) object;
                            if (mCountProposedSeqMap.containsKey(msgAtHoldQueue.getMsg()) &&
                                    mCountProposedSeqMap.get(msgAtHoldQueue.getMsg()) == receiveSocketReaderMap.size()) {
                                mCountProposedSeqMap.remove(msgAtHoldQueue.getMsg());
                                mSendQueue.add(new Pair<Integer, String>(0, msgAtHoldQueue.getSeqNumber() + ":" + msgAtHoldQueue.getMsg()));
                                continue;
                            }

                            //Remove the msg from the queue which was originated from crashed process
                            if (msgAtHoldQueue.getOriginationAVD() == maxDelayedAVD) {
                                ISISPriorQueue.remove(msgAtHoldQueue);
                            }
                        }
                        heartbeatMap.remove(maxDelayedAVD);
                    }
                }

                //Iterate through all the sockets available
                for (Map.Entry<Integer, BufferedReader> avdSocketEntry : receiveSocketReaderMap.entrySet()) {
                    try {
                        if(avdSocketEntry.getValue().ready()) {
                            //Read the msg from client
                            String msg = avdSocketEntry.getValue().readLine();
                            //If the incoming msg doesn't contains the sequence number then propose one
                            if(msg.equals("heartbeat")) {
                                heartbeatMap.put(avdSocketEntry.getKey(), System.currentTimeMillis());
                                continue;
                            }
                            if(!msg.contains(":")) {
                                mSeqNum++;
                                Log.e(TAG, "need to propose one for : " + msg + " with seq: " + (mSeqNum + (0.1f * myProcessNumber)));
                                ISISMsgBuffer msgAtHoldQueue = new ISISMsgBuffer(mSeqNum + (0.1f * myProcessNumber), msg, avdSocketEntry.getKey());
                                if(!ISISPriorQueue.contains(msgAtHoldQueue))
                                    ISISPriorQueue.add(msgAtHoldQueue);
                                mSendQueue.add(new Pair<Integer,String>(avdSocketEntry.getKey(), mSeqNum + (0.1f * myProcessNumber) + ":" + msg));
                            } else {
                                //Separate the sequence number and msg
                                String [] splitterMsg = msg.split(":");
                                msg = splitterMsg[1];
                                float msgSequenceNum = Float.parseFloat(splitterMsg[0]);
                                if(mSeqNum < (int) msgSequenceNum) {
                                    mSeqNum = (int) msgSequenceNum;
                                }

                                ISISMsgBuffer msgAtHoldQueue = new ISISMsgBuffer(msgSequenceNum, msg, myProcessNumber);
                                if(ISISPriorQueue.contains(msgAtHoldQueue)) {
                                    Log.e(TAG, "This msg is present at queue : " + msg + " with seq: " + msgSequenceNum);
                                    //access via for-loop to check the sequence number
                                    for(Object object : ISISPriorQueue) {
                                        msgAtHoldQueue = (ISISMsgBuffer) object;
                                        if(msgAtHoldQueue.getMsg().equals(msg)) {
                                            if(msgSequenceNum > msgAtHoldQueue.getSeqNumber())
                                                msgAtHoldQueue.setSeqNumber(msgSequenceNum);
                                            break;
                                        }
                                    }
                                } else {
                                    Log.e(TAG, "This msg is not present at queue : " + msg + " with seq: " + msgSequenceNum);
                                    ISISPriorQueue.add(msgAtHoldQueue);
                                }

                                if(mCountProposedSeqMap.containsKey(msg)) {
                                    mCountProposedSeqMap.replace(msg, mCountProposedSeqMap.get(msg) + 1);
                                    Log.e(TAG, "map count contains the msg: " + msg + " with count: " + mCountProposedSeqMap.get(msg));
                                    int maxProposedSequenceToReceived = isConnectionSetupNotCompleted? NUMBER_OF_AVDS : receiveSocketReaderMap.size();
                                    if(mCountProposedSeqMap.get(msg) == maxProposedSequenceToReceived) {
                                        Log.e(TAG, "removing from map count the msg: " + msg + " with seq: " + msgAtHoldQueue.getSeqNumber());
                                        mCountProposedSeqMap.remove(msg);
                                        mSendQueue.add(new Pair<Integer,String>(0, msgAtHoldQueue.getSeqNumber() + ":" + msg));
                                    }
                                    continue;
                                }

                                Log.e(TAG, "Marking the msg as deliverable msg: " + msgAtHoldQueue.getMsg() + " with seq: " + msgAtHoldQueue.getSeqNumber());
                                //Mark the priority queue entry as deliverable
                                msgAtHoldQueue.setIsDeliverable(true);

                                //Check the priority queue from head till all the item are deliverable
                                //for all of them insert the msg into db and remove the entries one by one
                                Object[] arr = ISISPriorQueue.toArray();
                                Arrays.sort(arr);
                                for(Object ob : arr) {
                                    Log.e(TAG, "The queue: " + ((ISISMsgBuffer)ob).getMsg()
                                            + " seq: "
                                            + ((ISISMsgBuffer)ob).getSeqNumber()
                                            + " del: "
                                            + ((ISISMsgBuffer)ob).mIsDeliverable
                                            + " orig: "
                                            + ((ISISMsgBuffer)ob).getOriginationAVD());
                                }
                                for(Object ob : arr) {
                                    if(!((ISISMsgBuffer)ob).getIsDeliverable()) {
                                        break;
                                    }
                                    ISISPriorQueue.remove((ISISMsgBuffer)ob);
                                    publishProgress(((ISISMsgBuffer)ob).getSeqNumber().toString(), ((ISISMsgBuffer)ob).getMsg());
                                }
                            }
                        }
                        for (Object object : ISISPriorQueue) {
                            ISISMsgBuffer msgAtHoldQueue = (ISISMsgBuffer) object;
                            int maxProposedSequenceToReceived = isConnectionSetupNotCompleted? NUMBER_OF_AVDS : receiveSocketReaderMap.size();
                            if (mCountProposedSeqMap.containsKey(msgAtHoldQueue.getMsg()) &&
                                    mCountProposedSeqMap.get(msgAtHoldQueue.getMsg()) == maxProposedSequenceToReceived) {
                                mCountProposedSeqMap.remove(msgAtHoldQueue.getMsg());
                                mSendQueue.add(new Pair<Integer, String>(0, msgAtHoldQueue.getSeqNumber() + ":" + msgAtHoldQueue.getMsg()));
                            }
                        }

                        mSendQueue.add(new Pair<Integer,String>(0, "heartbeat"));
                        Thread.sleep(100);
                    } catch (SocketTimeoutException st) {
                        Log.e(TAG, st.getMessage(), st);
                    } catch (IOException e) {
                        e.printStackTrace();
                        Log.e(TAG, e.getMessage(), e);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        Log.e(TAG, e.getMessage(), e);
                    } catch (Exception e) {
                        e.printStackTrace();
                        Log.e(TAG, e.getMessage(), e);
                    }
                }
            }
        }
        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }

        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strSeqNumber = mSeqNumForDb.toString();
            mSeqNumForDb++;
            String strReceived = strings[1].trim();

            ContentValues cv = new ContentValues();
            cv.put(GroupMessengerProvider.MessengerDbSchema.MessageTable.Cols.KEY, strSeqNumber);
            cv.put(GroupMessengerProvider.MessengerDbSchema.MessageTable.Cols.VALUE, strReceived);

            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            try {
                getApplicationContext().getContentResolver().insert(mUri, cv);

                StringBuilder  msgToShow = new StringBuilder ("");
                Cursor resultCursor = getApplicationContext().getContentResolver().query(mUri, null, null, null, null);
                if (resultCursor != null) {
                    if (resultCursor.moveToFirst()){
                        do{
                            String returnKey = resultCursor.getString(resultCursor.getColumnIndex(GroupMessengerProvider.MessengerDbSchema.MessageTable.Cols.KEY));
                            String returnValue = resultCursor.getString(resultCursor.getColumnIndex(GroupMessengerProvider.MessengerDbSchema.MessageTable.Cols.VALUE));
                            msgToShow.append(returnKey + " : " + returnValue + "\n");
                        }while(resultCursor.moveToNext());
                    }
                    resultCursor.close();
                }
                remoteTextView.setText(msgToShow.toString());
            } catch (Exception e) {
                Log.e(TAG, e.toString());
            }
            return;
        }
    }

    /***
     * Most of the below part code is been taken from PA1
     * SendEventTask is an AsyncTask that should handle multi casting events.
     *
     * Please make sure you understand how AsyncTask works by reading
     * http://developer.android.com/reference/android/os/AsyncTask.html
     */
    private class SendEventTask extends AsyncTask<Integer, String, Void> {

        private int mSeqNum = 0;
        @Override
        protected Void doInBackground(Integer... processNumber) {
            Integer procNumber = processNumber[0];
            HashMap<Integer, PrintWriter> sendSocketWriterMap = new HashMap<Integer, PrintWriter>();

            while(true) {
                while(!mSendQueue.isEmpty()) {
                    //Connect to all AVDs if not
                    //This loop will initialize when the application starts
                    while(sendSocketWriterMap.size() < NUMBER_OF_AVDS) {
                        for (String ReceiverPort : PORT_LIST) {
                            int avdNumber = ((Integer.parseInt(ReceiverPort)-11108)/4) + 1;
                            if(sendSocketWriterMap.containsKey(avdNumber)) {
                                Log.e(TAG, "Already established connection with Avd: " + avdNumber);
                                continue;
                            }

                            try {
                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(ReceiverPort));
                                socket.setSoTimeout(500);

                                //Create a socket writer to send data in the client socket to server
                                PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);

                                //Send msg to server
                                socketWriter.println(procNumber.toString());

                                sendSocketWriterMap.put(avdNumber, socketWriter);
                            } catch (UnknownHostException e) {
                                Log.e(TAG, "ClientTask UnknownHostException");
                            } catch (IOException e) {
                                Log.e(TAG, "ClientTask socket IOException");
                            }
                        }
                    }

                    Pair<Integer, String> msgEntry = mSendQueue.poll();
                    Integer currentAVD = msgEntry.first;
                    if(msgEntry.second.length() == 0)
                        continue;
                    try {
                        if(msgEntry.first == 0) {
                            //Iterate through all the sockets available
                            for (Map.Entry<Integer, PrintWriter> avdSocketEntry : sendSocketWriterMap.entrySet()) {
                                //Send msg to server
                                avdSocketEntry.getValue().println(msgEntry.second);
                                currentAVD++;
                            }
                        } else {
                            //Send msg to server
                            sendSocketWriterMap.get(msgEntry.first).println(msgEntry.second);
                        }
                    } catch (Exception e) {
                        sendSocketWriterMap.remove(currentAVD);
                        e.printStackTrace();
                        Log.e(TAG, e.getMessage(), e);
                    }
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
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
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            String msgToSend = msgs[0].trim().replaceAll("\n","");
            mCountProposedSeqMap.put(msgToSend, 0);
            mSendQueue.add(new Pair<Integer,String>(0, msgToSend));
            return null;
        }
    }
}