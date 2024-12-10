import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import React, { useRef, useState } from "react";

export const FileUpload = () => {
    const [file, setFile] = useState<File | null>(null)
    const fileInputRef = useRef<HTMLInputElement>(null)

    const handleFileDrop = (e: React.DragEvent<HTMLDivElement>) => {
        e.preventDefault()
        const droppedFile = e.dataTransfer.files[0]
        setFile(droppedFile)
    }

    const handleFileUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
        const uploadedFile = e.target.files?.[0]
        if(uploadedFile?.type !== 'application/dbase/.dbf') {
            alert('Tipo de arquivo inválido. Só é permitido arquivos .dbf')
            return
        }

        if (uploadedFile) {
            setFile(uploadedFile)
        }
    }

    const handleSelectFileClick = () => {
        fileInputRef.current?.click()
    }

    return (
        <section className="bg-background p-6 rounded-lg shadow">
            <h2 className="text-2xl font-bold mb-4">File Upload</h2>
            <div className="grid grid-cols-3 gap-4 mb-8">
                <Select>
                    <SelectTrigger>
                        <SelectValue placeholder="PA/RD"/>
                    </SelectTrigger>
                    <SelectContent>
                        <SelectItem value="PA">PA</SelectItem>
                        <SelectItem value="RD">RD</SelectItem>
                    </SelectContent>
                </Select>
                {/* <Select>
                    <SelectTrigger>
                        <SelectValue placeholder="Estado"/>
                    </SelectTrigger>
                    <SelectContent>
                        {dataEstadosBrasil.map((estado) => (
                            <SelectItem key={estado.value} value={estado.value}>
                                {estado.label}
                            </SelectItem>
                        ))}
                    </SelectContent>
                </Select> */}
            </div>
            <div
                className="border-2 border-dashed border-gray-300 rounded-lg p-8 text-center cursor-pointer mb-4"
                onDrop={handleFileDrop}
                onDragOver={(e) => e.preventDefault()}
            >
                <p>Drag and drop your file here, or click the button below to select a file</p>
                <input
                    type="file"
                    className="hidden"
                    onChange={handleFileUpload}
                    ref={fileInputRef}
                />
                <Button className="mt-4" onClick={handleSelectFileClick}>
                    Select File
                </Button>
            </div>
            {file && <p className="mb-4">Selected file: {file.name} / {file.size}kb</p>}
            <Button>Send File</Button>
        </section>
    )
}