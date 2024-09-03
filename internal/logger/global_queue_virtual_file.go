package logger

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"time"

	cbor "github.com/fxamacker/cbor/v2"
	"github.com/google/uuid"
	"github.com/klauspost/compress/s2"
	"github.com/minio/minio/internal/logger/target/http"
	"github.com/minio/minio/internal/store"
	"github.com/valyala/bytebufferpool"
)

func (g *GlobalQueue[I]) VirtualFileManager(ctx context.Context) {
	defer func() {
		r := recover()
		if r != nil {
			fmt.Println(r, string(debug.Stack()))
		}

		fmt.Println("disk manager exiting")
		if ctx.Err() != nil {
			time.Sleep(100 * time.Millisecond)
			go g.VirtualFileManager(ctx)
		}
	}()

	fmt.Println("STARTING DISK MANAGER")
	loopStart := time.Now()
	shouldDelete := false
	config := g.Config.Load()
	var size int64
	var err error

	for {
		config = g.Config.Load()

		time.Sleep((2 * time.Second) - time.Since(loopStart))
		loopStart = time.Now()

		for i := range g.Files {
			config = g.Config.Load()
			shouldDelete = false

			vf := g.Files[i].Load()
			if vf == nil {
				continue
			}

			fmt.Println("F:", config.name, i, time.Since(vf.Created).Milliseconds(), vf.entryCount, vf.TargetCount.Load(), vf.diskDir, vf.diskName, vf.Created.Format("15:04:05.000"))
			if time.Since(vf.Created).Seconds() < 3 {
				continue
			}

			if vf.entryCount == 0 {
				shouldDelete = true
			}

			if time.Since(vf.Created).Seconds() > config.dirMaxDuration.Seconds() {
				shouldDelete = true
			} else if !vf.Saved.IsZero() {
				if time.Since(vf.Saved).Seconds() > config.dirMaxDuration.Seconds() {
					shouldDelete = true
				}
			}

			if vf.TargetCount.Load() == 0 {
				shouldDelete = true
			}

			if shouldDelete {
				fmt.Println("RM:", i, config.name, vf.diskName)
				_ = g.removeFile(ctx, vf, i)
			} else if vf.diskDir != config.dir && config.dir != "" {
				fmt.Println("MV:", i, config.name, vf.diskName)
				_ = g.moveFile(ctx, vf, config.dir)
			}

		}

		if config.dir != "" {
			size, err = g.parseQueueDir(ctx)
			fmt.Println("SIZE POST PROCESSING:", size)
			if err != nil {
				g.logOnceIf(ctx, err, "dir_stats")
			} else {
				g.sizeOnDisk.Store(uint64(size))
			}
		}

	}
}

func (g *GlobalQueue[I]) parseQueueDir(ctx context.Context) (int64, error) {
	config := g.Config.Load()
	var size int64
	var files []fs.FileInfo
	var maxSizeInBytes int64 = int64(config.dirMaxSize) * 1_000_000

	err := filepath.Walk(config.dir, func(_ string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info == nil {
			return nil
		}
		if !info.IsDir() {
			if time.Since(info.ModTime()).Seconds() > config.dirMaxDuration.Seconds() {
				fmt.Println("DELETING FROM MOD TIME:", info.ModTime())
				err = os.Remove(config.dir + info.Name())
				if err != nil {
					g.logOnceIf(ctx, err, "file_delete")
				}
				return nil
			} else {
				size += info.Size()
				files = append(files, info)
			}
		}
		return nil
	})

	if size > int64(maxSizeInBytes) {

		slices.SortFunc(files, func(a fs.FileInfo, b fs.FileInfo) int {
			if time.Since(a.ModTime()) > time.Since(b.ModTime()) {
				return -1
			}
			return 1
		})

		for _, v := range files {
			if size > maxSizeInBytes {
				fmt.Println("DELETING FROM SIZE :", size, maxSizeInBytes)
				err = os.Remove(config.dir + v.Name())
				if err != nil {
					g.logOnceIf(ctx, err, "file_delete")
				} else {
					size -= v.Size()
				}
			}
		}
	}

	return size, err
}

func MigrateOldStore(cfg *http.Config) (err error) {
	queueStore := store.NewQueueStore[interface{}](
		filepath.Join(cfg.QueueDir, cfg.Name),
		uint64(cfg.QueueSize),
		httpLoggerExtension,
	)

	if err := queueStore.Open(); err != nil {
		return fmt.Errorf("unable to initialize the queue store of %s webhook: %w", cfg.Name, err)
	}
	return nil
}

func (g *GlobalQueue[I]) discardFile(index int) {
	fmt.Println("DS:", index, g.Config.Load().name)
	g.Files[index].Store(nil)
	return
}

func (g *GlobalQueue[I]) nextVirtualFile(ctx context.Context) (VF *VirtualFile[I], index int) {
	index = int(g.fileIndex.Add(1))
	if index == len(g.Files) {
		if g.fileIndex.CompareAndSwap(int32(index), 0) {
			index = 0
		} else {
			index = int(g.fileIndex.Add(1))
		}
	}

	fmt.Println("N:", index, g.Config.Load().name)
	oldFile := g.Files[index].Swap(&VirtualFile[I]{
		Entries: make([]*I, g.maxFileEntries),
		Created: time.Now(),
	})

	if oldFile != nil {
		fmt.Println("REMOVING OLD FILE:", oldFile.diskName)
		_ = g.removeFile(ctx, oldFile, index)
	}

	return g.Files[index].Load(), index
}

func (g *GlobalQueue[I]) moveFile(ctx context.Context, vf *VirtualFile[I], targetDir string) (err error) {
	err = os.Rename(vf.diskDir+vf.diskName, targetDir+vf.diskName)
	if err == nil {
		vf.diskDir = targetDir
	} else {
		g.logOnceIf(ctx,
			fmt.Errorf("Unable to move file from %s to %s .. err: %s ",
				err,
				vf.diskDir+vf.diskName,
				targetDir+vf.diskName,
			),
			"disk_mv")
	}
	return
}

func (g *GlobalQueue[I]) removeFile(ctx context.Context, vf *VirtualFile[I], index int) (err error) {
	if vf.diskName == "" {
		g.Files[index].CompareAndSwap(vf, nil)
		return nil
	}

	err = os.Remove(vf.diskDir + vf.diskName)
	if err != nil {
		g.logOnceIf(ctx,
			fmt.Errorf("Unable to remove file from disk: %s", err),
			"disk_remove")
	}
	g.Files[index].CompareAndSwap(vf, nil)
	return
}

func (g *GlobalQueue[I]) writeToDisk(ctx context.Context, vf *VirtualFile[I], targetDir string) (err error) {
	if targetDir == "" {
		return nil
	}

	encoderBuff := bytebufferpool.Get()
	defer bytebufferpool.Put(encoderBuff)
	encoder := cbor.NewEncoder(encoderBuff)

	vf.Saved = time.Now()
	vf.diskName = fmt.Sprintf("%d-%s.gz",
		vf.Saved.UnixNano(),
		uuid.NewString(),
	)

	vf.diskDir = targetDir
	f, err := os.Create(vf.diskDir + vf.diskName)
	if err != nil {
		err = fmt.Errorf("Unable to create file.. err: %s", err)
		g.logOnceIf(ctx, err, "write_file")
		return err
	}
	s2w := s2.NewWriter(f)

	for i := range vf.Entries {
		if vf.Entries[i] == nil {
			continue
		}

		err := encoder.Encode(vf.Entries[i])
		if err != nil {
			err = fmt.Errorf("Unable to encode file.. err: %s", err)
			g.logOnceIf(ctx, err, "file_encode")
			_ = os.Remove(vf.diskDir + vf.diskName)
			return err
		}

		n, err := s2w.Write(encoderBuff.Bytes())
		if err != nil {
			_ = os.Remove(vf.diskDir + vf.diskName)

			err = fmt.Errorf("Unable to compress file.. err:%s", err)
			g.logOnceIf(ctx, err, "file_compress")
			return err
		}

		if n != encoderBuff.Len() {
			_ = os.Remove(vf.diskDir + vf.diskName)

			err = fmt.Errorf("Unable to compress file, expected %d but compressed %d", encoderBuff.Len(), n)
			g.logOnceIf(ctx, err, "disk_write_compress")
			return err
		}

		encoderBuff.Reset()
	}

	s2w.Flush()

	return
}

func (g *GlobalQueue[I]) LoadFilesFromDisk(ctx context.Context) {
	config := g.Config.Load()

	if config == nil {
		g.logOnceIf(ctx, fmt.Errorf("No config found for global event queue"), "queue_full")
		return
	}

	if config.dir == "" {
		return
	}

	decompressedBuffer := bytebufferpool.Get()
	defer bytebufferpool.Put(decompressedBuffer)

	err := filepath.WalkDir(config.dir, func(path string, d fs.DirEntry, err error) error {
		if d == nil {
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}

		for i := range g.Files {
			f := g.Files[i].Load()
			if f != nil {
				if f.diskName == d.Name() {
					return nil
				}
			}
		}

		vf, index := g.nextVirtualFile(ctx)
		vf.Saved = time.Now()
		vf.diskDir = config.dir
		vf.diskName = d.Name()

		f, err := os.Open(path)
		if err != nil {

			_ = os.Remove(path)
			g.Files[index].Store(nil)

			g.logOnceIf(ctx, fmt.Errorf("Unable to open queue file on disk: %s", err), "file_read")

			return nil
		}

		s2w := s2.NewReader(f)
		s2w.DecodeConcurrent(decompressedBuffer, 1)
		rest := decompressedBuffer.Bytes()
		for len(rest) > 0 {
			vf.Entries[vf.entryCount] = new(I)
			rest, err = cbor.UnmarshalFirst(rest, vf.Entries[vf.entryCount])
			if err != nil {
				g.logOnceIf(ctx, fmt.Errorf("Unable to decode/uncompress queue file on disk: %s", err), "_decode")
			} else {
				vf.entryCount++
			}
		}

		g.sendVirtualFileToAllTargets(ctx, vf)

		return nil
	})
	if err != nil {
		g.logOnceIf(ctx, err, "dir_walk")
	}
}
