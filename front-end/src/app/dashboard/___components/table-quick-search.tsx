import {Input} from "@/components/ui/input";
import {Button} from "@/components/ui/button";
import {Search} from "lucide-react";
import {Table, TableBody, TableCell, TableHead, TableHeader, TableRow} from "@/components/ui/table";
import {
    Pagination,
    PaginationContent, PaginationEllipsis,
    PaginationItem,
    PaginationLink, PaginationNext,
    PaginationPrevious
} from "@/components/ui/pagination";
import React from "react";

export const TableQuickSearch = () => {

    // Sample data for the table
    const tableData = Array.from({ length: 10 }, (_, i) => ({
        id: i + 1,
        column1: `Data ${i + 1}A`,
        column2: `Data ${i + 1}B`,
        column3: `Data ${i + 1}C`,
    }))

    return (
        <section className="bg-background p-6 rounded-lg shadow">
            <h2 className="text-2xl font-bold mb-4">Busca Rápida</h2>
            <div className="flex items-center mb-4">
                <Input
                    placeholder="Search..."
                    className="max-w-sm"
                />
                <Button variant="ghost" size="icon" className="ml-2">
                    <Search className="h-4 w-4"/>
                </Button>
            </div>
            <Table>
                <TableHeader>
                    <TableRow>
                        <TableHead className={'w-1.5'}>ID</TableHead>
                        <TableHead className={'w-1/3'}>Index</TableHead>
                        <TableHead className={'w-1/3'}>Qnt. Documentos</TableHead>
                        <TableHead className={'w-1/3'}>Data Upload</TableHead>
                    </TableRow>
                </TableHeader>
                <TableBody>
                    {tableData.map((row) => (
                        <TableRow key={row.id}>
                            <TableCell>{row.id}</TableCell>
                            <TableCell>{row.column1}</TableCell>
                            <TableCell>{row.column2}</TableCell>
                            <TableCell>{row.column3}</TableCell>
                        </TableRow>
                    ))}
                </TableBody>
            </Table>
            <Pagination className="mt-4">
                <PaginationContent>
                    <PaginationItem>
                        <PaginationPrevious/>
                    </PaginationItem>
                    <PaginationItem>
                        <PaginationLink>1</PaginationLink>
                    </PaginationItem>
                    <PaginationItem>
                        <PaginationLink isActive>
                            2
                        </PaginationLink>
                    </PaginationItem>
                    <PaginationItem>
                        <PaginationLink>3</PaginationLink>
                    </PaginationItem>
                    <PaginationItem>
                        <PaginationEllipsis/>
                    </PaginationItem>
                    <PaginationItem>
                        <PaginationNext/>
                    </PaginationItem>
                </PaginationContent>
            </Pagination>
        </section>
    )
}