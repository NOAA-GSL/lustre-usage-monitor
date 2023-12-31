#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

extern "C" {
#include <assert.h>

#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stddef.h>
#include <fcntl.h>
#include <stdarg.h>
#include <dirent.h>
#include <stdint.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <libgen.h>

#include <zlib.h>
#include <lustre/lustreapi.h>
}

#include <algorithm>
#include <array>
#include <exception>
#include <sstream>
#include <utility>
#include <unordered_set>
#include <unordered_map>
#include <string>
#include <vector>
#include <fstream>
#include <memory>
#include <algorithm>

using namespace std;

////////////////////////////////////////////////////////////////////////

class GzFileHandler;
class DirResult;
class TopResult;
class DirWalkState;
class DiskUsage;
class FileError;
class ZlibFailed;
class BadFile;

template<class T> T above_int(T t,int i) {
  if(t<i)
    return T(i);
  else
    return t;
}

void ezwrite(gzFile out,const char *data,int64_t len);
void ezread(gzFile in,char *data,int64_t len);
void write_size_string(gzFile o,const string &s);
void read_size_string(gzFile in,string &s);
void be_verbose();
void be_quiet();
void set_restart_interval(int64_t seconds);
int debug(const char *format,...);
int info(const char *format,...);
int warning(const char *format,...);
int error(const char *format,...);
bool get_quota_info(const string &path,int64_t &size,int64_t &softlimit);
string random_base36_string();
string strdirname(char *path);
string strbasename(char *path);
string strrealpath(const string &s);
string strstrftime(const char *format,const struct tm *tm=NULL);
string strhostname();
bool write_report(const string &where,const DiskUsage &du,int64_t curspace,int64_t softlimit);

////////////////////////////////////////////////////////////////////////

class GzFileHandler {
public:
  GzFileHandler(const string &path,const string &tempfile,const string &mode);
  virtual ~GzFileHandler();
  void close();
  gzFile get() { return file; }
private:
  gzFile file;
  string path,mode,tempfile;
  bool have_tempfile;
};

struct FileInfo {
  int64_t bytes, blocks, files;
  FileInfo(): bytes(0), blocks(0), files(0) {}
  FileInfo(const FileInfo &o): bytes(o.bytes), blocks(o.blocks), files(o.files) {}
  FileInfo(std::initializer_list<int64_t> list) {
    auto it = list.begin();
    bytes = *it++;
    blocks = *it++;
    files = *it++;
  }
  FileInfo &operator += (const FileInfo &o) {
    bytes+=o.bytes;
    blocks+=o.blocks;
    files+=o.files;
    return *this;
  }
};

class DirResult {
  // A simple container that tracks the size of a directory
public:
  static const int age_count = 3;
  static const time_t age_seconds[age_count];

  DirResult() {};
  //  explicit DirResult(FileInfo f);
  explicit DirResult(gzFile in);
  DirResult(const DirResult &o): result(o.result) {}
  void write_restart(gzFile out) const;
  virtual ~DirResult() {}
  DirResult &add(int age,const FileInfo &fi);
  FileInfo &at_age(int age) { return result[age]; }
  const FileInfo &at_age(int age) const { return result[age]; }
  const array<FileInfo,age_count> &get_result() const { return result; }
  DirResult &operator += (const DirResult &d);
private:
  array<FileInfo,age_count> result;
};

class TopResult: public DirResult {
  // Tracks the size of a directory AND keeps the du output in an
  // ostringstream.
public:
  TopResult(const string &name,const string &out);
  TopResult(gzFile in);
  void write_restart(gzFile out) const;
  void write_content(const string &filename);

  virtual ~TopResult() {};
  string get_content() const { return content.str(); }
  ostringstream &add_content() { return content; }
  const string &get_name() const { return name; }
  time_t get_finish_time() const { return finish_time; }
  void set_finish_time() { finish_time=time(NULL); }
private:
  ostringstream content;
  string name;
  int64_t finish_time;
};

class DirWalkState: public DirResult {
  // Tracks the size of a directory, its name, and the list of
  // subdirectories already processed.
public:
  DirWalkState(const string &reldir,const string &path);
  DirWalkState(gzFile in);
  void write_restart(gzFile out) const;

  virtual ~DirWalkState() {}
  bool is_done(const string &d) const { return done.find(d)!=done.end(); }
  void mark_done(const string &d) { done.insert(d); }
  void clear_done() { done.clear(); }
  const string &get_reldir() const { return reldir; }
  const string &get_path() const { return path; }
private:

  string reldir, path;
  unordered_set<string> done;
};

class DiskUsage {
public:
  DiskUsage(const string &path,const string &restart_file,const string &output_base,bool restart=false);
  void insert_unseen(int64_t total_usage,int64_t total_files,const string &name);
  DirResult tree_walk();
  int64_t time_elapsed() const;
  void sort_results_by_size();
  typedef vector< shared_ptr<TopResult> >::iterator iterator;
  typedef vector< shared_ptr<TopResult> >::const_iterator const_iterator;
  iterator results_begin() { return ordered_results.begin(); }
  const_iterator results_begin() const { return ordered_results.begin(); }
  iterator results_end() { return ordered_results.end(); }
  const_iterator results_end() const { return ordered_results.end(); }
  const string &top_dir() const { return topdir; }
  shared_ptr<const TopResult> result_for(const string &name) const;
private:
  DirResult tree_walk(const string &reldir,const string &path,int walk_index,shared_ptr<TopResult>,bool restart);
  shared_ptr<TopResult> result_for(const string &name);
  void clear();
  bool read_restart();
  void read_restart_impl(gzFile file);
  bool write_restart() const;
  void write_restart_impl(gzFile file) const;
  bool should_write_restart() const;
private:
  mutable time_t last_restart_write=-1;
  int64_t start_time,end_time;
  vector<DirWalkState> state;
  unordered_set<ino_t> ino_seen;
  string topdir, restart_path, output_base;
  vector < shared_ptr<TopResult> > ordered_results;
  unordered_map < string,shared_ptr<TopResult> > hashed_results;
  bool have_read_restart;

  int64_t tick_time,tick_files;
  int64_t complaints,bad_ticks,failed_fstatat;

  static const int64_t magic_number=0x3031130330311303ll;
};

class FileError: public exception {
public:
  FileError() {}
  virtual ~FileError() {}
};

class ZlibFailed: public FileError {
public:
  ZlibFailed(const string &why): why(why) {}
  ZlibFailed(gzFile file) {
    int errnum=Z_ERRNO;
    const char *from_gzerror=NULL;

    if(file)
      from_gzerror=gzerror(file,&errnum);
    if(errnum==Z_ERRNO)
      why=strerror(errno);
    else if(from_gzerror && from_gzerror[0])
      why=from_gzerror;
    else if(from_gzerror && !from_gzerror[0] && errno)
      why=strerror(errno);
    else
      why="zlib call failed for no known reason (gzerror returned an empty string, and errno is zero)";
  }
  virtual ~ZlibFailed() {}
  virtual const char *what() const throw() {
    return why.c_str();
  }
private:
  string why;
};

class BadFile: public FileError {
public:
  BadFile(const string &s): why(s) {}
  virtual ~BadFile() {};
  virtual const char *what() const throw() { return why.c_str(); };
private:
  string why;
};

////////////////////////////////////////////////////////////////////////

// Globals

static int g_verbose=0;
static int64_t g_restart_interval=300;
static double g_min_files_per_second=0.1;
static time_t g_bad_node_sleep_time=120;
static bool g_ignore_failed_fstatat=false;

////////////////////////////////////////////////////////////////////////

// Functions

bool get_quota_info(const string &path,int64_t &size,int64_t &softlimit,int64_t &files,int64_t &file_softlimit) {
  string real_path=strrealpath(path.c_str());

  if(!real_path.size())
    return false; // already printed error message

  char fsname[PATH_MAX], mntdir[PATH_MAX];
  memset(fsname,0,PATH_MAX);
  memset(mntdir,0,PATH_MAX);
  if(llapi_search_mounts(real_path.c_str(),0,mntdir,fsname)) {
    error("ERROR: %s: cannot find lustre mount: %s\n",real_path.c_str(),strerror(errno));
    return false;
  }

  struct stat sb;
  if(stat(real_path.c_str(),&sb)) {
    error("ERROR: %s: cannot stat: %s\n",real_path.c_str(),strerror(errno));
    return false;
  }

  if_quotactl qctl;
  memset(&qctl,0,sizeof(if_quotactl));
  qctl.qc_cmd=LUSTRE_Q_GETQUOTA;
  qctl.qc_type=PRJQUOTA;
  qctl.qc_id=sb.st_gid;
  qctl.qc_dqblk.dqb_valid=1;
  if(llapi_quotactl(mntdir,&qctl)) {
    error("ERROR: %s: cannot get lustre quota info: %s\n",mntdir,strerror(errno));
    return false;
  }

  // fprintf(stderr,"%s: current=%llu hard=%llu soft=%llu for gid %llu\n",input_path.c_str(),
  //         (unsigned long long)qctl.qc_dqblk.dqb_curspace/1024,
  //         (unsigned long long)qctl.qc_dqblk.dqb_bhardlimit,
  //         (unsigned long long)qctl.qc_dqblk.dqb_bsoftlimit,sb.st_gid);

  softlimit=static_cast<int64_t>(1024ll*qctl.qc_dqblk.dqb_bsoftlimit);
  size=static_cast<int64_t>(qctl.qc_dqblk.dqb_curspace);
  file_softlimit=static_cast<int64_t>(1024ll*qctl.qc_dqblk.dqb_isoftlimit);
  files=static_cast<int64_t>(qctl.qc_dqblk.dqb_curinodes);
  return true;
}

string strdirname(const char *path) {
  size_t len=strlen(path)+1;
  unique_ptr<char[]> buf(new char[len+1>PATH_MAX+1 ? len+1 : PATH_MAX+1]);
  memcpy(buf.get(),path,len);
  char *result=dirname(buf.get());
  if(!result)
    return string(".");
  return string(result);
}

string strbasename(const char *path) {
  size_t len=strlen(path)+1;
  unique_ptr<char[]> buf(new char[len+1>PATH_MAX+1 ? len+1 : PATH_MAX+1]);
  memcpy(buf.get(),path,len);
  char *result=basename(buf.get());
  if(!result)
    return string(".");
  return string(result);
}

string strrealpath(const string &s) {
  unique_ptr<char[]> buf(new char[PATH_MAX+1]);
  memset(buf.get(),0,PATH_MAX+1);
  if(realpath(s.c_str(),buf.get()))
    return buf.get();
  string base=strbasename(s.c_str()),dir=strdirname(s.c_str());
  return strrealpath(dir)+'/'+base;
}

string strhostname() {
  unique_ptr<char[]> buf(new char[HOST_NAME_MAX+1]);
  memset(buf.get(),0,HOST_NAME_MAX+1);
  int result=gethostname(buf.get(),HOST_NAME_MAX);
  if(result && result!=ENAMETOOLONG && result!=EINVAL) {
    warning("gethostname failed: %s\n",strerror(errno));
    return "???";
  }
  buf.get()[HOST_NAME_MAX]='\0';
  return string(buf.get());
}

void ezwrite(gzFile out,const char *data,int64_t len) {
  const int64_t block_size=131072;
  while(len>0) {
    unsigned write=block_size;
    if(len<block_size)
      write=len;
    assert(write>0);
    int result=gzwrite(out,data,write);
    if(write<=0)
      throw ZlibFailed(out);
    else if((int64_t)result<(int64_t)write)
      throw ZlibFailed("short write (not enough disk space?)");
    data+=write;
    len-=write;
  }
}

void ezread(gzFile in,char *data,int64_t len) {
  const int64_t block_size=131072;
  while(len>0) {
    unsigned read=block_size;
    if(len<block_size)
      read=len;
    assert(read>0);
    int result=gzread(in,data,(int)read);
    if(result<0)
      throw ZlibFailed(in);
    else if((int64_t)result<(int64_t)read)
      throw ZlibFailed("unexpected end of file");
    data+=read;
    len-=read;
  }
}

void write_size_string(gzFile out,const string &s) {
  int64_t size=s.size()+1;
  ezwrite(out,(const char *)&size,8);
  ezwrite(out,s.c_str(),size);
}

void read_size_string(gzFile in,string &s) {
  int64_t size;
  ezread(in,(char *)&size,8);
  std::unique_ptr<char[]> buf(new char[size+1]);
  if(!buf)
    throw ZlibFailed("cannot allocate buffer for read");
  ezread(in,buf.get(),size);
  s=buf.get();
}

void be_verbose() {
  g_verbose=1;
}

void be_quiet() {
  g_verbose=-1;
}

void set_restart_interval(int64_t interval) {
  if(interval<10)
    warning("warning: minimum restart interval is 10 seconds; setting to 10\n");
  g_restart_interval=above_int(interval,10);
}

int debug(const char *format,...) {
  va_list ap;
  if(g_verbose<1)
    return 0;
  va_start(ap,format);
  int ret=vfprintf(stderr,format,ap);
  va_end(ap);
  return ret;
}

int info(const char *format,...) {
  va_list ap;
  if(g_verbose<0)
    return 0;
  va_start(ap,format);
  int ret=vfprintf(stderr,format,ap);
  va_end(ap);
  return ret;
}

int warning(const char *format,...) {
  va_list ap;
  if(g_verbose<-1)
    return 0;
  va_start(ap,format);
  int ret=vfprintf(stderr,format,ap);
  va_end(ap);
  return ret;
}

int error(const char *format,...) {
  va_list ap;
  if(g_verbose<-2)
    return 0;
  va_start(ap,format);
  int ret=vfprintf(stderr,format,ap);
  va_end(ap);
  return ret;
}

string random_base36_string() {
  const char *chars="0123456789abcdefghijklmnopqrstuvwxyz";
  char junk[13]={0};
  for(int i=0;i<12;i++)
    junk[i]=chars[(rand()/1000)%36];
  return junk;
}

string strstrftime(const char *format,const struct tm *tm) {
  struct tm my_tm;
  if(!tm) {
    time_t now=time(NULL);
    if(!gmtime_r(&now,&my_tm))
      memset(&my_tm,0,sizeof(tm));
    tm=&my_tm;
  }
  char buf[100]={0};
  if(0>=strftime(buf,100,format,tm))
    return string("???");
  return string(buf);
}

bool write_report(const string &where,const DiskUsage &du,int64_t curspace,int64_t softlimit,int64_t curfiles,int64_t fsoftlimit,bool short_report) {
  FILE *tgt=stdout;
  string tempfile("(*stdout*)");
  bool have_tempfile=false;

  if(where.size()) {
    have_tempfile=true;
    tempfile=where+"-"+random_base36_string()+".tmp";
    tgt=fopen(tempfile.c_str(),"wt");
    if(!tgt) {
      error("%s: cannot open for text writing: %s\n",tempfile.c_str(),strerror(errno));
      return false;
    }
  }

  shared_ptr<const TopResult> dot = du.result_for(".");
  int result;
  if(short_report)
    result=fprintf(tgt,"%-44s\t\t%5.1f\t\t%5.1f%%\t\t%5.1f\t\t%s\n",
                   strrealpath(du.top_dir()).c_str(),
                   double(curspace)/1048576.0/1048576,max(0.0,double(curspace)/softlimit*100),
                   double(softlimit)/1048576.0/1048576,strstrftime("%a %d %b %Y %T %Z").c_str());
  else
    result=fprintf(tgt,"%-44s\t\t%5.1f\t\t%5.1f%%\t\t%5.1f\t\t%s\t%8.1f\t%8.1f\t%10lld\t%8.1f%%\t%lld\n",
                   strrealpath(du.top_dir()).c_str(),
                   double(curspace)/1048576.0/1048576,max(0.0,double(curspace)/softlimit*100),
                   double(softlimit)/1048576.0/1048576,strstrftime("%a %d %b %Y %T %Z").c_str(),
                   dot->at_age(1).bytes/1048576.0/1048576,
                   dot->at_age(2).bytes/1048576.0/1048576,
                   static_cast<long long int>(curfiles),
                   max(0.0,double(curfiles)/max(1.0,double(fsoftlimit))*100),
                   static_cast<long long int>(fsoftlimit));
  if(result<=0) {
    error("%s: cannot write data: %s",tempfile.c_str(),strerror(errno));
    if(have_tempfile) unlink(tempfile.c_str());
    if(tgt) fclose(tgt);
    return false;
  }
  
  for(auto it=du.results_begin();it!=du.results_end();it++) {
    string name=(*it)->get_name();
    if(name==".")
      continue;
    time_t at_t=(*it)->get_finish_time();
    struct tm at_tm;
    if(!gmtime_r(&at_t,&at_tm))
      memset(&at_tm,0,sizeof(tm));
    string at=strstrftime("%a %d %b %Y %T %Z",&at_tm);
    if(short_report)
      fprintf(tgt,"%-43s \t\t%5.1f\t\t%5.1f%%\t\t\t\t%s\n",
              name.c_str(),(*it)->at_age(0).bytes/1048576.0/1048576,
              max(0.0,double((*it)->at_age(0).bytes)/softlimit)*100,at.c_str());
    else
      fprintf(tgt,"%-43s \t\t%5.1f\t\t%5.1f%%\t\t\t\t%s\t%8.1f\t%8.1f\t%10lld\t%8.1f%%\n",
              name.c_str(),(*it)->at_age(0).bytes/1048576.0/1048576,
              max(0.0,double((*it)->at_age(0).bytes)/softlimit)*100,at.c_str(),
              (*it)->at_age(1).bytes/1048576.0/1048576,
              (*it)->at_age(2).bytes/1048576.0/1048576,
              static_cast<long long int>((*it)->at_age(0).files),
              max(0.0,double(curfiles)/max(1.0,double(fsoftlimit))*100));
    if(result<=0) {
      error("%s: cannot write data: %s\n",tempfile.c_str(),strerror(errno));
      if(have_tempfile) unlink(tempfile.c_str());
      if(tgt) fclose(tgt);
      return false;
    }
  }

  if(have_tempfile && rename(tempfile.c_str(),where.c_str())) {
    error("%s: cannot rename to %s: %s\n",tempfile.c_str(),where.c_str(),strerror(errno));
    unlink(tempfile.c_str());
    if(tgt) fclose(tgt);
    return false;
  }

  if(tgt) fclose(tgt);
  return true;
}


////////////////////////////////////////////////////////////////////////

// Class GzFileHandler

GzFileHandler::GzFileHandler(const string &path,const string &tempfile,const string &mode):
  file(NULL),path(path),mode(mode),tempfile(tempfile),have_tempfile(tempfile.length()>0)
{
  const char *filename = have_tempfile ? tempfile.c_str() : path.c_str();
  file=gzopen(filename,mode.c_str());
  if(!file)
    throw ZlibFailed(file);
  if(gzbuffer(file,131072))
    throw ZlibFailed(file);
}

GzFileHandler::~GzFileHandler() {
  try {
    close();
  } catch(const ZlibFailed &z) {
    error("ERROR: failed to close a gzlib file: %s\n",z.what());
  } catch(const bad_alloc &b) {
    error("ERROR: bad_alloc; ran out of memory\n");
  } catch(...) {
    error("Uncaught C++ exception.\n");
  }
}

void GzFileHandler::close() {
  try {
    if(file) {
      int result=gzclose(file);
      file=NULL;
      switch(result) {
      case Z_STREAM_ERROR: throw ZlibFailed("Error on close: file is not valid");
      case Z_MEM_ERROR: throw ZlibFailed("Error on close: out of memory");
      case Z_BUF_ERROR: throw ZlibFailed("Error on close: last read ended in the middle of a gzip stream");
      case Z_OK: break;
      default: throw ZlibFailed("Unknown error on close.");
      };
    }
    if(have_tempfile) {
      if(rename(tempfile.c_str(),path.c_str()))
        throw ZlibFailed("Unable to rename gzipped file \""+tempfile+"\" to \""+path+"\": "+strerror(errno));
      have_tempfile=false;
    }
  } catch(const FileError &fe) {
    if(have_tempfile)
      unlink(tempfile.c_str());
    throw;
  }
}

////////////////////////////////////////////////////////////////////////

// Class DirResult

// File ages to track in seconds. Negative numbers means "all ages." Must be monotonically increasing.
const time_t DirResult::age_seconds[DirResult::age_count] = { -1, 7776000, 15552000 };

DirResult::DirResult(gzFile in)
{
  for(auto &bbf : result) {
    ezread(in,(char *)&bbf.bytes,8);
    ezread(in,(char *)&bbf.blocks,8);
    ezread(in,(char *)&bbf.files,8);
  }
}

void DirResult::write_restart(gzFile out) const {
  for(auto &bbf : result) {
    ezwrite(out,(const char *)&bbf.bytes,8);
    ezwrite(out,(const char *)&bbf.blocks,8);
    ezwrite(out,(const char *)&bbf.files,8);
  }
}

DirResult &DirResult::add(int age, const FileInfo &toAdd) {
  result[age] += toAdd;
  return *this;
}

DirResult &DirResult::operator += (const DirResult &d) {
  for(int i=0; i<age_count; ++i)
    result[i] += d.result[i];
  return *this;
}

////////////////////////////////////////////////////////////////////////

// Class TopResult

TopResult::TopResult(const string &name,const string &out):
  DirResult(),name(name),finish_time(-1)
{
  content<<out;
}
TopResult::TopResult(gzFile in):
  DirResult(in),content()
{
  ezread(in,(char*)&finish_time,8);
  read_size_string(in,name);
  string from;
  read_size_string(in,from);
  content<<from;
}
void TopResult::write_restart(gzFile out) const {
  this->DirResult::write_restart(out);
  ezwrite(out,(const char*)&finish_time,8);
  write_size_string(out,name);
  string c=get_content();
  write_size_string(out,c);
}
void TopResult::write_content(const string &filename) {
  debug("%s: write content for directory %s\n",filename.c_str(),name.c_str());
  GzFileHandler out(filename,filename+"-"+random_base36_string()+".tmp","wb");
  ezwrite(out.get(),content.str().c_str(),content.tellp());
  out.close();
  finish_time=time(NULL);
  info("%s: wrote content for directory %s\n",filename.c_str(),name.c_str());
}


////////////////////////////////////////////////////////////////////////

// Class DirWalkState

DirWalkState::DirWalkState(const string &reldir,const string &path):
  DirResult(), reldir(reldir), path(path), done()
{}

DirWalkState::DirWalkState(gzFile in):
  DirResult(in), reldir(), path(), done()
{
  read_size_string(in,reldir);
  read_size_string(in,path);
  int64_t size;
  ezread(in,(char*)&size,8);
  for(;size>0;size--) {
    string s;
    read_size_string(in,s);
    done.insert(s);
  }
}

void DirWalkState::write_restart(gzFile out) const {
  int64_t size;
  this->DirResult::write_restart(out);
  write_size_string(out,reldir);
  write_size_string(out,path);
  size=done.size();
  ezwrite(out,(const char *)&size,8);
  for(auto it=done.begin();it!=done.end();it++)
    write_size_string(out,*it);
}

////////////////////////////////////////////////////////////////////////

// class DiskUsage: small methods

DiskUsage::DiskUsage(const string &path,const string &restart_path,const string &output_base,bool restart):
  topdir(path), restart_path(restart_path), output_base(output_base),
  have_read_restart(false),
  tick_time(0),tick_files(0),complaints(0),bad_ticks(0),failed_fstatat(0)
{
  start_time=time(NULL);
  last_restart_write=start_time;
  end_time=-1;

  if(restart) {
    if(!read_restart())
      warning("warning: %s: failed to restart; will rerun instead\n",restart_path.c_str());
    else
      have_read_restart=true;
  }
}

shared_ptr<TopResult> DiskUsage::result_for(const string &name) {
  auto there=hashed_results.find(name);
  if(there!=hashed_results.end() && there->second)
    return there->second;
  shared_ptr<TopResult> newwed(new TopResult(name,""));
  ordered_results.emplace_back(newwed);
  hashed_results[name]=newwed;
  return ordered_results.back();
}

shared_ptr<const TopResult> DiskUsage::result_for(const string &name) const {
  auto there=hashed_results.find(name);
  if(there!=hashed_results.end() && there->second)
    return there->second;
  return nullptr;
}

void DiskUsage::clear() {
  start_time=time(NULL);
  last_restart_write=start_time;
  state.clear();
  ino_seen.clear();
  ordered_results.clear();
  hashed_results.clear();
}

int64_t DiskUsage::time_elapsed() const {
  return end_time-start_time;
}

void DiskUsage::insert_unseen(int64_t total_usage,int64_t total_files,const string &name) {
  int64_t total_seen=0;
  int64_t total_fseen=0;
  for(auto it=ordered_results.begin();it!=ordered_results.end();it++) {
    if((*it)->get_name()== ".")
      continue;
    total_seen += (*it)->at_age(0).bytes;
    total_fseen += (*it)->at_age(0).files;
  }
  int64_t unseen=above_int(total_usage-total_seen,0);
  int64_t funseen=above_int(total_files-total_fseen,0);
  shared_ptr<TopResult> tr=result_for(name);
  tr->add(0, {unseen,(unseen+511)/512,funseen});
  tr->set_finish_time();
  sort_results_by_size();
  debug("unseen = %lld = (%lld - %lld)\n",(long long)tr->at_age(0).bytes,(long long)total_usage,(long long)total_seen);
  debug("funseen = %lld = (%lld - %lld)\n",(long long)tr->at_age(0).files,(long long)total_files,(long long)total_fseen);
}

DirResult DiskUsage::tree_walk() {
  end_time=time(NULL);
  int walk_index=state.empty() ? -1 : 0;
  DirResult result=tree_walk(".",topdir,walk_index,result_for("."),have_read_restart);
  end_time=time(NULL);
  sort_results_by_size();
  return result;
}

bool DiskUsage::should_write_restart() const {
  return time(NULL)-last_restart_write >= g_restart_interval;
}

void DiskUsage::sort_results_by_size() {
  stable_sort(ordered_results.begin(),ordered_results.end(),
              [](const shared_ptr<TopResult> &left,const shared_ptr<TopResult> &right) {
                return right->at_age(0).bytes<left->at_age(0).bytes;
              });
}

////////////////////////////////////////////////////////////////////////

// class DiskUsage: restart files

bool DiskUsage::write_restart() const {
  debug("%s: write restart\n",restart_path.c_str());
  string tempname=restart_path+"-"+random_base36_string()+".tmp";
  try {
    GzFileHandler out(restart_path,tempname,"wb");
    write_restart_impl(out.get());
    out.close();
  } catch(const FileError &fe) {
    error("ERROR: %s: failed to write restart: %s\n",restart_path.c_str(),fe.what());
    return false;
  }
  
  last_restart_write=time(NULL);
  info("%s: wrote RESTART at %s\n",restart_path.c_str(),strstrftime("%a %d %b %Y %T %Z").c_str());
  
  return true;
}

void DiskUsage::write_restart_impl(gzFile out) const {
  int64_t size;

  size=magic_number;
  ezwrite(out,(const char *)(&size),8);
  write_size_string(out,topdir);

  int64_t elapsed=time(NULL)-start_time;
  ezwrite(out,(const char *)(&elapsed),8);

  size=state.size();
  ezwrite(out,(const char *)(&size),8);
  for(auto it=state.begin();it!=state.end();it++)
    it->write_restart(out);

  size=(int64_t)ino_seen.size();
  ezwrite(out,(const char *)&size,8);
  for(auto it=ino_seen.begin();it!=ino_seen.end();it++) {
    size=*it;
    ezwrite(out,(const char *)(&size),8);
  }

  size=(int64_t)ordered_results.size();
  ezwrite(out,(const char *)&size,8);
  for(auto it=ordered_results.begin();it!=ordered_results.end();it++)
    (*it)->write_restart(out);
}

bool DiskUsage::read_restart() {
  try {
    GzFileHandler in(restart_path,"","rb");
    read_restart_impl(in.get());
    in.close();
    return true;
  } catch(const FileError &fe) {
    clear();
    warning("warning: %s: cannot read restart: %s\n",restart_path.c_str(),fe.what());
    return false;
  }
}

void DiskUsage::read_restart_impl(gzFile in) {
  int64_t size;
  ezread(in,(char *)&size,8);
  if(size!=magic_number)
    throw BadFile("wrong magic number");

  string newtop;
  read_size_string(in,newtop);
  if(topdir!=newtop)
    throw BadFile("top dir in restart file ("+newtop+") does not match specified path ("+topdir+")");

  int64_t elapsed;
  ezread(in,(char *)&elapsed,8);

  ezread(in,(char *)&size,8);
  debug("%s: expect %lld states\n",restart_path.c_str(),(long long)size);
  for(;size>0;size--)
    state.emplace_back(in);

  ezread(in,(char *)&size,8);
  debug("%s: expect %lld ino elements\n",restart_path.c_str(),(long long)size);
  for(int64_t idx=0;size>0;idx++,size--) {
    int64_t ino;
    ezread(in,(char *)&ino,8);
    ino_seen.insert((ino_t)ino);
  }
  
  ezread(in,(char *)&size,8);
  for(int64_t idx=0;size>0;idx++,size--) {
    shared_ptr<TopResult> newwed(new TopResult(in));
    ordered_results.emplace_back(newwed);
    hashed_results[newwed->get_name()]=newwed;
  }
  start_time=time(NULL)-elapsed;
}

////////////////////////////////////////////////////////////////////////

// class DiskUsage: tree_walk (the meat of the program)

DirResult DiskUsage::tree_walk(const string &reldir,const string &path,
                               int walk_index,shared_ptr<TopResult> tr,bool restart) {
  assert(reldir.find('/') == string::npos);
  if(restart)
    info("restart state[%d] = %s\n",walk_index,path.c_str());
  assert(path.size());
  if(!restart) {
    walk_index=state.size();
    state.emplace_back(reldir,path);
    state.back().clear_done();
  }
  if(walk_index==0) {
    tick_time=time(NULL);
    tick_files=0;
  }
  assert(walk_index>=0);
  if(restart) {
    if((int64_t)state.size()>(walk_index+1)) {
      string sub_reldir(state[walk_index+1].get_reldir());
      string sub_path(state[walk_index+1].get_path());
      printf("sub_reldir=%s sub_path=%s\n",sub_reldir.c_str(),sub_path.c_str());
      shared_ptr<TopResult> sub_tr=(walk_index==0) ? result_for(sub_reldir) : tr;
      DirResult result=tree_walk(sub_reldir,sub_path,walk_index+1,sub_tr,restart);
      state[walk_index].mark_done(sub_reldir);
      state[walk_index]+=result;
      if(walk_index==0) {
        string filename = output_base+sub_reldir+".du.gz";
        *tr += result;
        try {
          sub_tr->write_content(filename);
        } catch(const FileError &fe) {
          assert(false);
          error("ERROR: %s: %s\n",filename.c_str(),fe.what());
          error("ERROR: Cannot write output files in run area. This is an unrecoverable error.\n");
          error("ERROR: Potential existential crisis: did the disk usage monitor run out of disk space?\n");
          exit(2);
        }
      }
    } else
      write_restart();
  }

  DIR *dir=opendir(path.c_str());
  if(!dir) {
    warning("warning: %s: cannot open dir; will ignore\n",path.c_str());
    state.pop_back();
    return DirResult();
  }

  if(!restart && walk_index==0)
    write_restart();

  struct dirent *dent;
  while( (dent=readdir(dir)) ) {
    bool skip=false;
    if(restart && state[walk_index].is_done(dent->d_name))
      continue; // Already processed this before restart

    if(dent->d_name[0]=='.') {
      if(dent->d_name[1]=='.' && dent->d_name[2]=='\0')
        continue;
      else if(dent->d_name[1]=='\0')
        skip=true;
    }

    if(ino_seen.find(dent->d_ino)!=ino_seen.end() && !skip) {
      debug("%s/%s: already saw inode number %llu",path.c_str(),dent->d_name,(unsigned long long)dent->d_ino);
      continue; // Don't handle hard links twice
    }

    state[walk_index].mark_done(dent->d_name);
    ino_seen.insert(dent->d_ino);

    if(dent->d_type == DT_DIR && !skip) {
      string substr=path;
      if(path.back()=='/') 
        substr+=dent->d_name;
      else {
        substr+='/';
        substr+=dent->d_name;
      }
      shared_ptr<TopResult> sub_tr=(walk_index==0) ? result_for(dent->d_name) : tr;
      DirResult result=tree_walk(dent->d_name,substr,-1,sub_tr,false);
      state[walk_index]+=result;
      assert(state[walk_index].at_age(2).bytes >= result.at_age(2).bytes);
      if(walk_index==0)
        *tr += result;
    } else {
      struct stat sb;
      if(fstatat(dirfd(dir),dent->d_name,&sb,AT_SYMLINK_NOFOLLOW)) {
        warning("warning: %s/%s: cannot fstatat; will ignore: %s\n",path.c_str(),dent->d_name,strerror(errno));
        if(!g_ignore_failed_fstatat)
          failed_fstatat++;
        continue;
      }

      shared_ptr<TopResult> result;
      time_t age = max<time_t>(0,time(NULL) - max(sb.st_mtime,sb.st_atime));
      for(int iage=0; iage<DirResult::age_count; ++iage) {
        if(age > DirResult::age_seconds[iage]) {
          state[walk_index].add(iage, {sb.st_size,sb.st_blocks,1});
          if(walk_index==0 && !skip) {
            // Top level result entry for a file
            if(!result)
              result=result_for(dent->d_name);
            result->add(iage, {sb.st_size,sb.st_blocks,1});
            result->set_finish_time();
          }
          tr->add(iage, {sb.st_size,sb.st_blocks,1});
        }
      }
      tick_files++;
    }

    if(should_write_restart()) {
      time_t now=time(NULL);
      write_restart();
      double rate=tick_files/double(now-tick_time);
      info("%s: am here at %f files/sec\n",path.c_str(),rate);
      int64_t unhappiness = bad_ticks*20 + failed_fstatat;
      string host=strhostname();
      if(unhappiness>=20 && complaints<1) {
        complaints++;
        warning("warning: %s: possible bad node\n",host.c_str());
        if(failed_fstatat>0)
          warning("warning: %s: %lld fstatat calls have failed\n",
                  host.c_str(),(long long)failed_fstatat);
        if(rate<g_min_files_per_second)
          warning("warning: %s: %f files/s is below minimum allowed %f\n",
                  host.c_str(),rate,g_min_files_per_second);
        if(g_bad_node_sleep_time>0) {
          warning("warning: will sleep %lld seconds and hope the node fixes itself...\n",
                  g_bad_node_sleep_time);
          sleep(g_bad_node_sleep_time);
          warning("... done sleeping.\n");
        }
      } else if(unhappiness>=40) {
        complaints++;
        error("ERROR: %s: likely a bad node. Will abort.\n",host.c_str());
        if(failed_fstatat>0)
          error("ERROR: %s: %lld fstatat calls have failed\n",
                host.c_str(),(long long)failed_fstatat);
        if(rate<g_min_files_per_second)
          error("ERROR: %s: %f files/s is below minimum allowed %f\n",
                host.c_str(),rate,g_min_files_per_second);
        error("ERROR: will exit with status 3 now.\n");
        exit(3);
      }
      tick_time=now;
      tick_files=0;
    }
  } // directory loop
  closedir(dir);
  DirResult &result = state[walk_index];
  for(auto & fr : result.get_result())
    tr->add_content() << fr.bytes << '\t';
  tr->add_content() << result.at_age(0).files << '\t' << path << endl;
  debug("%lld\t%s\n",(long long)state[walk_index].at_age(0).bytes,path.c_str());
  state.pop_back();
  if(walk_index==1) {
    string filename=output_base+reldir+".du.gz";
    try {
      tr->write_content(filename);
    } catch(const FileError &fe) {
      assert(false);
      error("ERROR: %s: %s\n",filename.c_str(),fe.what());
      error("ERROR: Cannot write output files in run area. This is an unrecoverable error.\n");
      error("ERROR: Potential existential crisis: did the disk usage monitor run out of disk space?\n");
      exit(2);
    }
  }
  return result;
} // tree_walk

  ////////////////////////////////////////////////////////////////////////

void usage() {
  error("SYNTAX: lustre-disk-usage [-f] [-d done_file] [-o report_file] [-m rate] [-r] [-q] [-v] [-t restart_interval] /path/to/dir /path/to/restart.gz /string/prepended/to/output\n"
        "Analyzes disk usage in a project within a Lustre filesystem.\n"
        "\n"
        "  -f = if the done_file exists, delete it and run anyway.\n"
        "  -d done_file = write this file upon successful exit. Do not start if this file exists.\n"
        "  -o report_file = write the text report table to this file instead of stdout\n"
        "  -s short_report_file = write out report only up to the date column to this file\n"
        "  -m rate = minimum files per second before triggering an exit (real value).\n"
        "  -r = restart from the /path/to/restart.gz file if possible, otherwise start over.\n"
        "  -q = be quiet; only prints errors and warnings.\n"
        "  -v = be extremely verbose; only useful for debugging.\n"
        "  -t restart_interval = write restart files this often (seconds; minimum 10)\n"
        "  -i = do not abort due to large numbers of failed fstatat calls (needed on Hera scratch1)\n"
        "\n"
        "/path/to/dir = the directory whose usage you want\n"
        "/path/to/restart.gz = name of the gzipped restart file\n"
        "/string/prepended/to/output = prepend this to the name of the gzipped inventory files\n");
}

int main(int argc,char **argv) {
  bool restart=false,force=false;
  string done_file,report_file,short_report;

  int opt;
  while( (opt=getopt(argc,argv,"s:ilfrqvd:t:o:m:")) != -1 ) {
    switch(opt) {
    case 'r':     restart=true;                         break;
    case 'f':     force=true;                           break;
    case 'q':     be_quiet();                           break;
    case 'v':     be_verbose();                         break;
    case 't':     set_restart_interval(atoi(optarg));   break;
    case 'd':     done_file=optarg;                     break;
    case 's':     short_report=optarg;                  break;
    case 'o':     report_file=optarg;                   break;
    case 'm':     g_min_files_per_second=atof(optarg);  break;
    case 'i':     g_ignore_failed_fstatat=true;         break;
    default:
      usage();
      return 2;
    }
  }

  if(argc!=optind+3) {
    usage();
    return 2;
  }

  string input_path(argv[optind]);
  string restart_path=strrealpath(argv[optind+1]);
  string output_prepend(argv[optind+2]);

  debug("input_path=\"%s\"=\"%s\" restart_path=\"%s\" output_prepend=\"%s\"\n",
        input_path.c_str(),input_path.c_str(),restart_path.c_str(),output_prepend.c_str());

  struct stat sb;
  if(done_file.size() && !stat(done_file.c_str(),&sb)) {
    if(force) {
      if(unlink(done_file.c_str())) {
        error("ERROR: %s: unable to unlink donefile: %s\n",done_file.c_str(),strerror(errno));
        return 1;
      }
    } else {
      error("ERROR: Donefile \"%s\" already exists. Use -f to force a rerun.\n",done_file.c_str());
      return 0;
    }
  }

  if(restart)
    info("Restart from %s\n",argv[optind+1]);

  DiskUsage du(input_path,restart_path,output_prepend,restart);
  DirResult result=du.tree_walk();
  int64_t elapsed=above_int(du.time_elapsed(),1);

  int64_t curspace,softlimit;
  int64_t curfiles,fsoftlimit;
  if(!get_quota_info(input_path,curspace,softlimit,curfiles,fsoftlimit))
    return 1; // already printed error message

  du.insert_unseen(curspace,curfiles,"--unseen-by-du--");

  if(!write_report(report_file,du,curspace,softlimit,curfiles,fsoftlimit,false))
    return 1; // already printed error message

  if(!short_report.empty() && !write_report(short_report,du,curspace,softlimit,curfiles,fsoftlimit,true))
    return 1; // already printed error message

  info("Processed %lld files in %lld seconds (%.2f per second)\n",
       (long long)result.at_age(0).files,(long long)elapsed,
       (double)result.at_age(0).files/(double)elapsed);

  if(done_file.size()) {
    ofstream f(done_file.c_str());
    f<<strstrftime("%a %d %b %Y %T %Z")<<endl;
  }

  return 0;
}
