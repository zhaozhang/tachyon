package tachyon.client;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.UnderFileSystem;
import tachyon.client.table.RawTable;
import tachyon.conf.CommonConf;
import tachyon.conf.UserConf;
import tachyon.master.MasterClient;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.ClientDependencyInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.ClientRawTableInfo;
import tachyon.thrift.ClientWorkerInfo;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.NetAddress;
import tachyon.util.CommonUtils;
import tachyon.worker.WorkerClient;

/**
 * Tachyon's user client API. It contains a MasterClient and several WorkerClients
 * depending on how many workers the client program is interacting with.
 */
public class TachyonFS extends AbstractTachyonFS {
  /**
   * Create a TachyonFS handler.
   * 
   * @param tachyonPath
   *          a Tachyon path contains master address. e.g., tachyon://localhost:19998,
   *          tachyon://localhost:19998/ab/c.txt
   * @return the corresponding TachyonFS handler
   * @throws IOException
   */
  public static synchronized TachyonFS get(final String tachyonPath) throws IOException {
    boolean zookeeperMode = false;
    String tempAddress = tachyonPath;
    if (tachyonPath.startsWith(Constants.HEADER)) {
      tempAddress = tachyonPath.substring(Constants.HEADER.length());
    } else if (tachyonPath.startsWith(Constants.HEADER_FT)) {
      zookeeperMode = true;
      tempAddress = tachyonPath.substring(Constants.HEADER_FT.length());
    } else {
      throw new IOException("Invalid Path: " + tachyonPath + ". Use " + Constants.HEADER
          + "host:port/ ," + Constants.HEADER_FT + "host:port/");
    }
    String masterAddress = tempAddress;
    if (tempAddress.contains(Constants.PATH_SEPARATOR)) {
      masterAddress = tempAddress.substring(0, tempAddress.indexOf(Constants.PATH_SEPARATOR));
    }
    Preconditions.checkArgument(masterAddress.split(":").length == 2,
        "Illegal Tachyon Master Address: " + tachyonPath);

    String masterHost = masterAddress.split(":")[0];
    int masterPort = Integer.parseInt(masterAddress.split(":")[1]);
    return new TachyonFS(new InetSocketAddress(masterHost, masterPort), zookeeperMode);
  }

  /**
   * Create a TachyonFS handler.
   * 
   * @param masterHost
   *          master host details
   * @param masterPort
   *          port master listens on
   * @param zookeeperMode
   *          use zookeeper
   * 
   * @return the corresponding TachyonFS hanlder
   * @throws IOException
   */
  public static synchronized TachyonFS
      get(String masterHost, int masterPort, boolean zookeeperMode) throws IOException {
    return new TachyonFS(new InetSocketAddress(masterHost, masterPort), zookeeperMode);
  }

  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  private final long USER_QUOTA_UNIT_BYTES = UserConf.get().QUOTA_UNIT_BYTES;

  private final int USER_FAILED_SPACE_REQUEST_LIMITS = UserConf.get().FAILED_SPACE_REQUEST_LIMITS;
  // The RPC client talks to the system master.
  private MasterClient mMasterClient = null;
  // The Master address.
  private InetSocketAddress mMasterAddress = null;
  // The RPC client talks to the local worker if there is one.
  private WorkerClient mWorkerClient = null;
  // Whether use ZooKeeper or not
  private boolean mZookeeperMode;
  // Cached ClientFileInfo
  private Map<String, ClientFileInfo> mPathToClientFileInfo =
      new HashMap<String, ClientFileInfo>();
  private Map<Integer, ClientFileInfo> mIdToClientFileInfo =
      new HashMap<Integer, ClientFileInfo>();

  private UnderFileSystem mUnderFileSystem = null;

  // All Blocks has been locked.
  private Map<Long, Set<Integer>> mLockedBlockIds = new HashMap<Long, Set<Integer>>();

  // Each user facing block has a unique block lock id.
  private AtomicInteger mBlockLockId = new AtomicInteger(0);

  // Available memory space for this client.
  private Long mAvailableSpaceBytes;

  private TachyonFS(InetSocketAddress masterAddress, boolean zookeeperMode) throws IOException {
    mMasterAddress = masterAddress;
    mZookeeperMode = zookeeperMode;
    mAvailableSpaceBytes = 0L;

    mMasterClient = new MasterClient(mMasterAddress, mZookeeperMode);
    mWorkerClient = new WorkerClient(mMasterClient);
  }

  /**
   * Update the latest block access time on the worker.
   * 
   * @param blockId
   *          the local block's id
   * @throws IOException
   */
  private synchronized void accessLocalBlock(long blockId) throws IOException {
    if (mWorkerClient.isLocal()) {
      mWorkerClient.accessBlock(blockId);
    }
  }

  /**
   * Notify the worker that the checkpoint file of the file FID has been added.
   * 
   * @param fid
   *          the file id
   * @throws IOException
   */
  synchronized void addCheckpoint(int fid) throws IOException {
    mWorkerClient.addCheckpoint(mMasterClient.getUserId(), fid);
  }

  /**
   * Notify the worker to checkpoint the file asynchronously
   * 
   * @param fid
   *          the file id
   * @return true if succeed, false otherwise
   * @throws IOException
   */
  synchronized boolean asyncCheckpoint(int fid) throws IOException {
    return mWorkerClient.asyncCheckpoint(fid);
  }

  /**
   * Notify the worker the block is cached.
   * 
   * @param blockId
   *          the block id
   * @throws IOException
   */
  public synchronized void cacheBlock(long blockId) throws IOException {
    mWorkerClient.cacheBlock(mMasterClient.getUserId(), blockId);
  }

  /**
   * Cleans the given path, throwing an IOException rather than an InvalidPathException.
   * 
   * @param path
   *          The path to clean
   * @return the cleaned path
   */
  private synchronized String cleanPathIOException(String path) throws IOException {
    try {
      return CommonUtils.cleanPath(path);
    } catch (InvalidPathException e) {
      throw new IOException(e);
    }
  }

  /**
   * Close the client. Close the connections to both to master and worker
   * 
   * @throws IOException
   */
  @Override
  public synchronized void close() throws IOException {
    if (mWorkerClient.isConnected()) {
      mWorkerClient.returnSpace(mMasterClient.getUserId(), mAvailableSpaceBytes);
      mWorkerClient.close();
    }

    if (mMasterClient != null) {
      mMasterClient.close();
    }
  }

  /**
   * The file is complete.
   * 
   * @param fid
   *          the file id
   * @throws IOException
   */
  synchronized void completeFile(int fid) throws IOException {
    mMasterClient.user_completeFile(fid);
  }

  /**
   * Create a user local temporary folder and return it
   * 
   * @return the local temporary folder for the user
   * @throws IOException
   */
  synchronized File createAndGetUserLocalTempFolder() throws IOException {
    String userTempFolder = mWorkerClient.getUserTempFolder();

    if (userTempFolder == null) {
      return null;
    }

    File ret = new File(userTempFolder);
    if (!ret.exists()) {
      if (ret.mkdir()) {
        CommonUtils.changeLocalFileToFullPermission(ret.getAbsolutePath());
        LOG.info("Folder " + ret + " was created!");
      } else {
        LOG.error("Failed to create folder " + ret);
        return null;
      }
    }

    return ret;
  }

  /**
   * Create a user UnderFileSystem temporary folder and return it
   * 
   * @return the UnderFileSystem temporary folder
   * @throws IOException
   */
  synchronized String createAndGetUserUfsTempFolder() throws IOException {
    String tmpFolder = mWorkerClient.getUserUfsTempFolder();
    if (tmpFolder == null) {
      return null;
    }

    if (mUnderFileSystem == null) {
      mUnderFileSystem = UnderFileSystem.get(tmpFolder);
    }

    mUnderFileSystem.mkdirs(tmpFolder, true);

    return tmpFolder;
  }

  /**
   * Create a Dependency
   * 
   * @param parents
   *          the dependency's input files
   * @param children
   *          the dependency's output files
   * @param commandPrefix
   * @param data
   * @param comment
   * @param framework
   * @param frameworkVersion
   * @param dependencyType
   *          the dependency's type, Wide or Narrow
   * @param childrenBlockSizeByte
   *          the block size of the dependency's output files
   * @return the dependency's id
   * @throws IOException
   */
  public synchronized int createDependency(List<String> parents, List<String> children,
      String commandPrefix, List<ByteBuffer> data, String comment, String framework,
      String frameworkVersion, int dependencyType, long childrenBlockSizeByte) throws IOException {
    return mMasterClient.user_createDependency(parents, children, commandPrefix, data, comment,
        framework, frameworkVersion, dependencyType, childrenBlockSizeByte);
  }

  /**
   * Create a RawTable and return its id
   * 
   * @param path
   *          the RawTable's path
   * @param columns
   *          number of columns it has
   * @return the id if succeed, -1 otherwise
   * @throws IOException
   */
  public synchronized int createRawTable(String path, int columns) throws IOException {
    return createRawTable(path, columns, ByteBuffer.allocate(0));
  }

  /**
   * Create a RawTable and return its id
   * 
   * @param path
   *          the RawTable's path
   * @param columns
   *          number of columns it has
   * @param metadata
   *          the meta data of the RawTable
   * @return the id if succeed, -1 otherwise
   * @throws IOException
   */
  public synchronized int createRawTable(String path, int columns, ByteBuffer metadata)
      throws IOException {
    String cleanedPath = cleanPathIOException(path);
    if (columns < 1 || columns > CommonConf.get().MAX_COLUMNS) {
      throw new IOException("Column count " + columns + " is smaller than 1 or " + "bigger than "
          + CommonConf.get().MAX_COLUMNS);
    }

    return mMasterClient.user_createRawTable(cleanedPath, columns, metadata);

  }

  /**
   * Return whether the file exists or not
   * 
   * @param path
   *          the file's path in Tachyon file system
   * @return true if it exists, false otherwise
   * @throws IOException
   */
  public synchronized boolean exist(String path) throws IOException {
    return getFileId(path) != -1;
  }

  /**
   * Get the block id by the file id and block index. it will check whether the file and the block
   * exist.
   * 
   * @param fid
   *          the file id
   * @param blockIndex
   *          The index of the block in the file.
   * @return the block id if exists
   * @throws IOException
   *           if the file does not exist, or connection issue.
   */
  public synchronized long getBlockId(int fid, int blockIndex) throws IOException {
    ClientFileInfo info = mIdToClientFileInfo.get(fid);
    if (info == null) {
      info = getFileStatus(fid, "");
      if (info != null) {
        mIdToClientFileInfo.put(fid, info);
      } else {
        throw new IOException("File " + fid + " does not exist.");
      }
    }

    if (info.blockIds.size() > blockIndex) {
      return info.blockIds.get(blockIndex);
    }

    return mMasterClient.user_getBlockId(fid, blockIndex);
  }

  /**
   * Get the block id by the file id and offset. it will check whether the file and the block exist.
   * 
   * @param fid
   *          the file id
   * @param offset
   *          The offset of the file.
   * @return the block id if exists
   * @throws IOException
   */
  synchronized long getBlockIdBasedOnOffset(int fid, long offset) throws IOException {
    ClientFileInfo info;
    if (!mIdToClientFileInfo.containsKey(fid)) {
      info = getFileStatus(fid, "");
      mIdToClientFileInfo.put(fid, info);
    }
    info = mIdToClientFileInfo.get(fid);

    int index = (int) (offset / info.getBlockSizeByte());

    return getBlockId(fid, index);
  }

  /**
   * @return a new block lock id
   */
  public int getBlockLockId() {
    return mBlockLockId.getAndIncrement();
  }

  /**
   * @param fid
   *          the file id
   * @return the block size of the file, in bytes
   */
  public synchronized long getBlockSizeByte(int fid) {
    return mIdToClientFileInfo.get(fid).getBlockSizeByte();
  }

  /**
   * Return the under filesystem path of the file
   * 
   * @param fid
   *          the file id
   * @return the checkpoint path of the file
   * @throws IOException
   */
  synchronized String getUfsPath(int fid) throws IOException {
    ClientFileInfo info = mIdToClientFileInfo.get(fid);
    if (info == null || !info.getUfsPath().equals("")) {
      info = getFileStatus(fid, "");
      mIdToClientFileInfo.put(fid, info);
    }

    return info.getUfsPath();
  }

  /**
   * Get a ClientBlockInfo by the file id and block index
   * 
   * @param fid
   *          the file id
   * @param blockIndex
   *          The index of the block in the file.
   * @return the ClientBlockInfo of the specified block
   * @throws IOException
   */
  public synchronized ClientBlockInfo getClientBlockInfo(int fid, int blockIndex)
      throws IOException {
    boolean fetch = false;
    if (!mIdToClientFileInfo.containsKey(fid)) {
      fetch = true;
    }
    ClientFileInfo info = null;
    if (!fetch) {
      info = mIdToClientFileInfo.get(fid);
      if (info.isFolder || info.blockIds.size() <= blockIndex) {
        fetch = true;
      }
    }

    if (fetch) {
      info = getFileStatus(fid, "");
      mIdToClientFileInfo.put(fid, info);
    }

    if (info == null) {
      throw new IOException("File " + fid + " does not exist.");
    }
    if (info.isFolder) {
      throw new IOException(new FileDoesNotExistException("File " + fid + " is a folder."));
    }
    if (info.blockIds.size() <= blockIndex) {
      throw new IOException("BlockIndex " + blockIndex + " is out of the bound in file " + info);
    }

    try {
      return mMasterClient.user_getClientBlockInfo(info.blockIds.get(blockIndex));
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    } catch (BlockInfoException e) {
      throw new IOException(e);
    }
  }

  /**
   * Get a ClientDependencyInfo by the dependency id
   * 
   * @param depId
   *          the dependency id
   * @return the ClientDependencyInfo of the specified dependency
   * @throws IOException
   */
  public synchronized ClientDependencyInfo getClientDependencyInfo(int depId) throws IOException {
    return mMasterClient.getClientDependencyInfo(depId);
  }

  /**
   * Get the creation time of the file
   * 
   * @param fid
   *          the file id
   * @return the creation time in milliseconds
   */
  public synchronized long getCreationTimeMs(int fid) {
    return mIdToClientFileInfo.get(fid).getCreationTimeMs();
  }

  /**
   * Get <code>TachyonFile</code> based on the file id.
   * 
   * NOTE: This *will* use cached file metadata, and so will not see changes to dynamic properties,
   * such as the pinned flag. This is also different from the behavior of getFile(path), which
   * by default will not use cached metadata.
   * 
   * @param fid
   *          file id.
   * @return TachyonFile of the file id, or null if the file does not exist.
   */
  public synchronized TachyonFile getFile(int fid) throws IOException {
    return getFile(fid, true);
  }

  /**
   * Get <code>TachyonFile</code> based on the file id. If useCachedMetadata, this will not see
   * changes to the file's pin setting, or other dynamic properties.
   * 
   * @return TachyonFile of the file id, or null if the file does not exist.
   */
  public synchronized TachyonFile getFile(int fid, boolean useCachedMetadata) throws IOException {
    if (!useCachedMetadata || !mIdToClientFileInfo.containsKey(fid)) {
      ClientFileInfo clientFileInfo = getFileStatus(fid, "");
      if (clientFileInfo == null) {
        return null;
      }
      mIdToClientFileInfo.put(fid, clientFileInfo);
    }
    return new TachyonFile(this, fid);
  }

  /**
   * Get <code>TachyonFile</code> based on the path. Does not utilize the file metadata cache.
   * 
   * @param path
   *          file path.
   * @return TachyonFile of the path, or null if the file does not exist.
   * @throws IOException
   */
  public synchronized TachyonFile getFile(String path) throws IOException {
    return getFile(path, false);
  }

  /**
   * Get <code>TachyonFile</code> based on the path. If useCachedMetadata, this will not see
   * changes to the file's pin setting, or other dynamic properties.
   */
  public synchronized TachyonFile getFile(String path, boolean useCachedMetadata)
      throws IOException {
    String cleanedPath = cleanPathIOException(path);
    ClientFileInfo clientFileInfo = getFileStatus(cleanedPath, useCachedMetadata);
    if (clientFileInfo == null) {
      return null;
    }
    mIdToClientFileInfo.put(clientFileInfo.getId(), clientFileInfo);
    return new TachyonFile(this, clientFileInfo.getId());
  }

  /**
   * Get all the blocks' ids of the file
   * 
   * @param fid
   *          the file id
   * @return the list of blocks' ids
   * @throws IOException
   */
  public synchronized List<Long> getFileBlockIdList(int fid) throws IOException {
    ClientFileInfo info = mIdToClientFileInfo.get(fid);
    if (info == null || !info.isComplete) {
      info = getFileStatus(fid, "");
      mIdToClientFileInfo.put(fid, info);
    }

    if (info == null) {
      throw new IOException("File " + fid + " does not exist.");
    }

    return info.blockIds;
  }

  /**
   * Get all the blocks' info of the file
   * 
   * @param fid
   *          the file id
   * @return the list of the blocks' info
   * @throws IOException
   */
  public synchronized List<ClientBlockInfo> getFileBlocks(int fid) throws IOException {
    // TODO Should read from mClientFileInfos if possible. Should add timeout to improve this.
    return mMasterClient.user_getFileBlocks(fid);
  }

  /**
   * Get the net address of all the file's hosts
   * 
   * @param fileId
   *          the file id
   * @return the list of all the file's hosts, in String
   * @throws IOException
   */
  public synchronized List<String> getFileHosts(int fileId) throws IOException {
    List<NetAddress> adresses = getFileNetAddresses(fileId);
    List<String> ret = new ArrayList<String>(adresses.size());
    for (NetAddress address : adresses) {
      ret.add(address.mHost);
      if (address.mHost.endsWith(".ec2.internal")) {
        ret.add(address.mHost.substring(0, address.mHost.length() - 13));
      }
    }

    return ret;
  }

  /**
   * Get file id by the path. It will check if the path exists.
   * 
   * @param path
   *          the path in Tachyon file system
   * @return the file id if exists, -1 otherwise
   * @throws IOException
   */
  public synchronized int getFileId(String path) throws IOException {
    try {
      return mMasterClient.getFileStatus(-1, cleanPathIOException(path)).getId();
    } catch (IOException e) {
      return -1;
    }
  }

  public synchronized List<NetAddress> getFileNetAddresses(int fileId) throws IOException {
    List<NetAddress> ret = new ArrayList<NetAddress>();
    List<ClientBlockInfo> blocks = mMasterClient.user_getFileBlocks(fileId);
    Set<NetAddress> locationSet = new HashSet<NetAddress>();
    for (ClientBlockInfo info : blocks) {
      locationSet.addAll(info.getLocations());
    }
    ret.addAll(locationSet);

    return ret;
  }

  public synchronized List<List<String>> getFilesHosts(List<Integer> fileIds) throws IOException {
    List<List<String>> ret = new ArrayList<List<String>>();
    for (int k = 0; k < fileIds.size(); k ++) {
      ret.add(getFileHosts(fileIds.get(k)));
    }

    return ret;
  }

  public synchronized List<List<NetAddress>> getFilesNetAddresses(List<Integer> fileIds)
      throws IOException {
    List<List<NetAddress>> ret = new ArrayList<List<NetAddress>>();
    for (int k = 0; k < fileIds.size(); k ++) {
      ret.add(getFileNetAddresses(fileIds.get(k)));
    }

    return ret;
  }

  /**
   * Returns the local filename for the block if that file exists on the local file system. This is
   * an alpha power-api feature for applications that want short-circuit-read files directly. There
   * is no guarantee that the file still exists after this call returns, as Tachyon may evict blocks
   * from memory at any time.
   * 
   * @param blockId
   *          The id of the block.
   * @return filename on local file system or null if file not present on local file system.
   */
  String getLocalFilename(long blockId) throws IOException {
    String rootFolder = getRootFolder();
    if (rootFolder != null) {
      String localFileName = CommonUtils.concat(rootFolder, blockId);
      File file = new File(localFileName);
      if (file.exists()) {
        return localFileName;
      }
    }
    return null;
  }

  /**
   * Get the number of blocks of a file. Only works when the file exists.
   * 
   * @param fid
   *          the file id
   * @return the number of blocks if the file exists
   * @throws IOException
   */
  synchronized int getNumberOfBlocks(int fid) throws IOException {
    ClientFileInfo info = mIdToClientFileInfo.get(fid);
    if (info == null || !info.isComplete) {
      info = getFileStatus(fid, "");
      mIdToClientFileInfo.put(fid, info);
    }
    if (info == null) {
      throw new IOException("File " + fid + " does not exist.");
    }
    return info.getBlockIds().size();
  }

  /**
   * @param fid
   *          the file id
   * @return the path of the file in Tachyon file system, null if the file does not exist.
   */
  synchronized String getPath(int fid) {
    return mIdToClientFileInfo.get(fid).getPath();
  }

  /**
   * Get the RawTable by id
   * 
   * @param id
   *          the id of the raw table
   * @return the RawTable
   * @throws IOException
   */
  public synchronized RawTable getRawTable(int id) throws IOException {
    ClientRawTableInfo clientRawTableInfo = mMasterClient.user_getClientRawTableInfoById(id);
    return new RawTable(this, clientRawTableInfo);
  }

  /**
   * Get the RawTable by path
   * 
   * @param path
   *          the path of the raw table
   * @return the RawTable
   * @throws IOException
   */
  public synchronized RawTable getRawTable(String path) throws IOException {
    String cleanedPath = cleanPathIOException(path);
    ClientRawTableInfo clientRawTableInfo =
        mMasterClient.user_getClientRawTableInfoByPath(cleanedPath);
    return new RawTable(this, clientRawTableInfo);
  }

  /**
   * @return the local root data folder
   * @throws IOException
   */
  synchronized String getRootFolder() throws IOException {
    return mWorkerClient.getDataFolder();
  }

  /**
   * @return the address of the UnderFileSystem
   * @throws IOException
   */
  public synchronized String getUfsAddress() throws IOException {
    return mMasterClient.user_getUfsAddress();
  }

  /**
   * @return all the works' info
   * @throws IOException
   */
  public synchronized List<ClientWorkerInfo> getWorkersInfo() throws IOException {
    return mMasterClient.getWorkersInfo();
  }

  /**
   * @return true if there is a local worker, false otherwise
   * @throws IOException
   */
  public synchronized boolean hasLocalWorker() throws IOException {
    return mWorkerClient.isLocal();
  }

  /**
   * @param fid
   *          the file id
   * @return true if this file is complete, false otherwise
   * @throws IOException
   */
  synchronized boolean isComplete(int fid) throws IOException {
    if (!mIdToClientFileInfo.get(fid).isComplete) {
      mIdToClientFileInfo.put(fid, getFileStatus(fid, ""));
    }
    return mIdToClientFileInfo.get(fid).isComplete;
  }

  /**
   * @return true if this client is connected to master, false otherwise
   */
  public synchronized boolean isConnected() {
    return mMasterClient.isConnected();
  }

  /**
   * @param fid
   *          the file id
   * @return true if the file is a directory, false otherwise
   */
  synchronized boolean isDirectory(int fid) {
    return mIdToClientFileInfo.get(fid).isFolder;
  }

  /**
   * Return whether the file is in memory or not. Note that a file may be partly in memory. This
   * value is true only if the file is fully in memory.
   * 
   * @param fid
   *          the file id
   * @return true if the file is fully in memory, false otherwise
   * @throws IOException
   */
  synchronized boolean isInMemory(int fid) throws IOException {
    ClientFileInfo info = getFileStatus(fid, "");
    mIdToClientFileInfo.put(info.getId(), info);
    return 100 == info.inMemoryPercentage;
  }

  /**
   * @param fid
   *          the file id
   * @return true if the file is need pin, false otherwise
   */
  synchronized boolean isNeedPin(int fid) {
    return mIdToClientFileInfo.get(fid).isPinned;
  }

  /**
   * Lock a block in the current TachyonFS.
   * 
   * @param blockId
   *          The id of the block to lock. <code>blockId</code> must be positive.
   * @param blockLockId
   *          The block lock id of the block of lock. <code>blockLockId</code> must be non-negative.
   * @return true if successfully lock the block, false otherwise (or invalid parameter).
   */
  private synchronized boolean lockBlock(long blockId, int blockLockId) throws IOException {
    if (blockId <= 0 || blockLockId < 0) {
      return false;
    }

    if (mLockedBlockIds.containsKey(blockId)) {
      mLockedBlockIds.get(blockId).add(blockLockId);
      return true;
    }

    if (!mWorkerClient.isLocal()) {
      return false;
    }
    mWorkerClient.lockBlock(blockId, mMasterClient.getUserId());

    Set<Integer> lockIds = new HashSet<Integer>(4);
    lockIds.add(blockLockId);
    mLockedBlockIds.put(blockId, lockIds);
    return true;
  }

  /**
   * Read local block return a TachyonByteBuffer
   * 
   * @param blockId
   *          The id of the block.
   * @param offset
   *          The start position to read.
   * @param len
   *          The length to read. -1 represents read the whole block.
   * @return <code>TachyonByteBuffer</code> containing the block.
   * @throws IOException
   */
  TachyonByteBuffer readLocalByteBuffer(long blockId, long offset, long len) throws IOException {
    if (offset < 0) {
      throw new IOException("Offset can not be negative: " + offset);
    }
    if (len < 0 && len != -1) {
      throw new IOException("Length can not be negative except -1: " + len);
    }

    int blockLockId = getBlockLockId();
    if (!lockBlock(blockId, blockLockId)) {
      return null;
    }
    String localFileName = getLocalFilename(blockId);
    if (localFileName != null) {
      try {
        RandomAccessFile localFile = new RandomAccessFile(localFileName, "r");

        long fileLength = localFile.length();
        String error = null;
        if (offset > fileLength) {
          error = String.format("Offset(%d) is larger than file length(%d)", offset, fileLength);
        }
        if (error == null && len != -1 && offset + len > fileLength) {
          error =
              String.format("Offset(%d) plus length(%d) is larger than file length(%d)", offset,
                  len, fileLength);
        }
        if (error != null) {
          localFile.close();
          throw new IOException(error);
        }

        if (len == -1) {
          len = fileLength - offset;
        }

        FileChannel localFileChannel = localFile.getChannel();
        ByteBuffer buf = localFileChannel.map(FileChannel.MapMode.READ_ONLY, offset, len);
        localFileChannel.close();
        localFile.close();
        accessLocalBlock(blockId);
        return new TachyonByteBuffer(this, buf, blockId, blockLockId);
      } catch (FileNotFoundException e) {
        LOG.info(localFileName + " is not on local disk.");
      } catch (IOException e) {
        LOG.warn("Failed to read local file " + localFileName + " because:", e);
      }
    }

    unlockBlock(blockId, blockLockId);
    return null;
  }

  public synchronized void releaseSpace(long releaseSpaceBytes) {
    mAvailableSpaceBytes += releaseSpaceBytes;
  }

  /**
   * Report the lost file to master
   * 
   * @param fileId
   *          the lost file id
   * @throws IOException
   */
  public synchronized void reportLostFile(int fileId) throws IOException {
    mMasterClient.user_reportLostFile(fileId);
  }

  /**
   * Request the dependency's needed files
   * 
   * @param depId
   *          the dependency id
   * @throws IOException
   */
  public synchronized void requestFilesInDependency(int depId) throws IOException {
    mMasterClient.user_requestFilesInDependency(depId);
  }

  /**
   * Try to request space from worker. Only works when a local worker exists.
   * 
   * @param requestSpaceBytes
   *          the space size in bytes
   * @return true if succeed, false otherwise
   * @throws IOException
   */
  public synchronized boolean requestSpace(long requestSpaceBytes) throws IOException {
    if (!mWorkerClient.isLocal()) {
      return false;
    }
    int failedTimes = 0;
    while (mAvailableSpaceBytes < requestSpaceBytes) {
      long toRequestSpaceBytes =
          Math.max(requestSpaceBytes - mAvailableSpaceBytes, USER_QUOTA_UNIT_BYTES);
      if (mWorkerClient.requestSpace(mMasterClient.getUserId(), toRequestSpaceBytes)) {
        mAvailableSpaceBytes += toRequestSpaceBytes;
      } else {
        LOG.info("Failed to request " + toRequestSpaceBytes + " bytes local space. " + "Time "
            + (failedTimes ++));
        if (failedTimes == USER_FAILED_SPACE_REQUEST_LIMITS) {
          return false;
        }
      }
    }

    if (mAvailableSpaceBytes < requestSpaceBytes) {
      return false;
    }

    mAvailableSpaceBytes -= requestSpaceBytes;

    return true;
  }

  /**
   * Print out the string representation of this Tachyon server address.
   * 
   * @return the string representation like tachyon://host:port or tachyon-ft://host:port
   */
  @Override
  public String toString() {
    return (mZookeeperMode ? Constants.HEADER_FT : Constants.HEADER) + mMasterAddress.toString();
  }

  /**
   * Unlock a block in the current TachyonFS.
   * 
   * @param blockId
   *          The id of the block to unlock. <code>blockId</code> must be positive.
   * @param blockLockId
   *          The block lock id of the block of unlock. <code>blockLockId</code> must be
   *          non-negative.
   * @return true if successfully unlock the block with <code>blockLockId</code>,
   *         false otherwise (or invalid parameter).
   */
  synchronized boolean unlockBlock(long blockId, int blockLockId) throws IOException {
    if (blockId <= 0 || blockLockId < 0) {
      return false;
    }

    if (!mLockedBlockIds.containsKey(blockId)) {
      return true;
    }
    Set<Integer> lockIds = mLockedBlockIds.get(blockId);
    lockIds.remove(blockLockId);
    if (!lockIds.isEmpty()) {
      return true;
    }

    if (!mWorkerClient.isLocal()) {
      return false;
    }

    mWorkerClient.unlockBlock(blockId, mMasterClient.getUserId());
    mLockedBlockIds.remove(blockId);

    return true;
  }

  /**
   * Sets the "pinned" flag for the given file. Pinned files are never evicted
   * by Tachyon until they are unpinned.
   * 
   * Calling setPinned() on a folder will recursively set the "pinned" flag on
   * all of that folder's children. This may be an expensive operation for
   * folders with many files/subfolders.
   */
  public synchronized void setPinned(int fid, boolean pinned) throws IOException {
    mMasterClient.user_setPinned(fid, pinned);
  }

  /** Alias for setPinned(fid, true). */
  public synchronized void pinFile(int fid) throws IOException {
    setPinned(fid, true);
  }

  /** Alias for setPinned(fid, false). */
  public synchronized void unpinFile(int fid) throws IOException {
    setPinned(fid, false);
  }

  /** Returns true if the given file or folder has its "pinned" flag set. */
  public synchronized boolean isPinned(int fid, boolean useCachedMetadata) throws IOException {
    ClientFileInfo info;
    if (!useCachedMetadata || !mIdToClientFileInfo.containsKey(fid)) {
      info = getFileStatus(fid, "");
      mIdToClientFileInfo.put(fid, info);
    }
    info = mIdToClientFileInfo.get(fid);

    return info.isPinned;
  }

  /**
   * Update the RawTable's meta data
   * 
   * @param id
   *          the raw table's id
   * @param metadata
   *          the new meta data
   * @throws IOException
   */
  public synchronized void updateRawTableMetadata(int id, ByteBuffer metadata) throws IOException {
    mMasterClient.user_updateRawTableMetadata(id, metadata);
  }

  @Override
  public synchronized int createFile(String path, String ufsPath, long blockSizeByte,
      boolean recursive) throws IOException {
    return mMasterClient.user_createFile(path, ufsPath, blockSizeByte, recursive);
  }

  @Override
  public synchronized boolean delete(int fileId, String path, boolean recursive)
      throws IOException {
    return mMasterClient.user_delete(fileId, path, recursive);
  }

  @Override
  public synchronized boolean rename(int fileId, String srcPath, String dstPath)
      throws IOException {
    return mMasterClient.user_rename(fileId, srcPath, dstPath);
  }

  @Override
  public synchronized boolean mkdirs(String path, boolean recursive) throws IOException {
    return mMasterClient.user_mkdirs(path, recursive);
  }

  @Override
  public synchronized List<ClientFileInfo> listStatus(String path) throws IOException {
    return mMasterClient.listStatus(path);
  }

  /**
   * Get a ClientFileInfo of the file
   * 
   * @param path
   *          the file path in Tachyon file system
   * @param useCachedMetadata
   *          if true use the local cached meta data
   * @return the ClientFileInfo
   * @throws IOException
   */
  private synchronized ClientFileInfo getFileStatus(String path, boolean useCachedMetadata)
      throws IOException {
    ClientFileInfo ret = null;
    String cleanedPath = cleanPathIOException(path);
    if (useCachedMetadata && mPathToClientFileInfo.containsKey(cleanedPath)) {
      return mPathToClientFileInfo.get(path);
    }
    try {
      ret = getFileStatus(-1, cleanedPath);
    } catch (IOException e) {
      LOG.warn(e.getMessage() + cleanedPath, e);
      return null;
    }

    // TODO LRU on this Map.
    if (ret != null && useCachedMetadata) {
      mPathToClientFileInfo.put(cleanedPath, ret);
    } else {
      mPathToClientFileInfo.remove(cleanedPath);
    }

    return ret;
  }

  @Override
  public ClientFileInfo getFileStatus(int fileId, String path) throws IOException {
    return mMasterClient.getFileStatus(fileId, path);
  }
}
