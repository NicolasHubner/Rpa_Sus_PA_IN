'use client';
import { cn } from '@/lib/utils';
import { Button } from '@radix-ui/themes';
import { useCallback, useState } from 'react';
import { useDropzone } from 'react-dropzone';
const FileUpload = () => {
  const [files, setFiles] = useState<File[]>([]);

  // Handle file drop
  const onDrop = useCallback((acceptedFiles: any) => {
    console.log(acceptedFiles);
    setFiles([...files, ...acceptedFiles]);
    // acceptedFiles.forEach((f) => {
    //   const reader = new FileReader()

    //   reader.onabort = () => console.log('file reading was aborted')
    //   reader.onerror = () => console.log('file reading has failed')
    //   reader.onload = () => {
    //   // Do whatever you want with the file contents
    //     const binaryStr = reader.result
    //     console.log(binaryStr)
    //   }
    //   reader.readAsArrayBuffer(f)
    // })
    
  }, [])

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    multiple: false,
    onDragEnter: () => console.log('onDragEnter'),
    onDragOver: () => console.log('onDragOver'),
    onDragLeave: () => console.log('onDragLeave'),
    // accept: {
    //   'application/dbase': ['.dbf']
    // }
  });

  return (
    <div className="p-8">
      {/* Drag and drop zone */}
      <div
        {...getRootProps()}
        className={cn(
          'flex justify-center items-center p-10 border-2 border-dashed rounded-lg cursor-pointer',
          isDragActive ? 'border-green-500 bg-green-50' : 'border-gray-300'
        )}
      >
        <input {...getInputProps()} />
        {isDragActive ? (
          <p className="text-green-500">Drop the files here ...</p>
        ) : (
          <p>Drag & drop some files here, or click to select files</p>
        )}
      </div>

      {/* File list */}
      <div className="mt-4 space-y-2">
        {files.length > 0 && <h3 className="text-lg font-medium">Uploaded Files:</h3>}
        <ul>
          {files.map((file, index) => (
            <li key={index} className="p-2 bg-gray-100 rounded">
              {file.name} ({(file.size / 1024).toFixed(2)} KB)
            </li>
          ))}
        </ul>
      </div>

      {/* Upload Button */}
      <Button className="mt-4" onClick={() => console.log('Uploading...')}>Upload Files</Button>
    </div>
  );
};

export default FileUpload;
