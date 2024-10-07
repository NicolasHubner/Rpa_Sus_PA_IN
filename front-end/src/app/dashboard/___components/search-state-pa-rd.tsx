import {Select, SelectContent, SelectItem, SelectTrigger, SelectValue} from "@/components/ui/select";
import {Button} from "@/components/ui/button";
import {Table, TableBody, TableCell, TableHead, TableHeader, TableRow} from "@/components/ui/table";
import React from "react";
import {dataEstadosBrasil} from "@/utils/constants/state-brasil";
import {dataMesBrasil} from "@/utils/constants/months-brasil";
import {Card, CardContent} from "@/components/ui/card";

export const SearchStatePaRd = () => {
    const arrayYearsFromNowTo1989 = Array.from({length: new Date().getFullYear() - 1989}, (_, i) => i + 1989);

    const total = 'R$ 1.000,00'
    return (
    <section className="bg-background p-6 rounded-lg shadow">
        <h2 className="text-2xl font-bold mb-4">Busca Estado PA/RD</h2>
        <div className="grid grid-cols-3 gap-4 mb-4">
            <Select>
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
            </Select>
            <Select>
                <SelectTrigger>
                    <SelectValue placeholder="Mes"/>
                </SelectTrigger>
                <SelectContent>
                    {dataMesBrasil.map((mes) => (
                        <SelectItem key={mes.value} value={mes.value}>
                            {mes.label}
                        </SelectItem>
                    ))}
                </SelectContent>
            </Select>
            <Select>
                <SelectTrigger>
                    <SelectValue placeholder="Ano"/>
                </SelectTrigger>
                <SelectContent>
                    {arrayYearsFromNowTo1989.map((ano) => (
                        <SelectItem key={ano} value={String(ano)}>
                            {ano}
                        </SelectItem>
                    ))}
                </SelectContent>
            </Select>
        </div>
        <Button className="mb-4">Pesquisar</Button>
        <Table>
            <TableHeader>
                <TableRow>
                    <TableHead className={'w-1/3'}>Tipo</TableHead>
                    <TableHead className={'w-1/3'}>Qnt. Registro</TableHead>
                    <TableHead className={'w-1/3'}>Total</TableHead>
                </TableRow>
            </TableHeader>
            <TableBody>
                <TableRow>
                    <TableCell>PA</TableCell>
                    <TableCell>Data 1B</TableCell>
                    <TableCell>Data 1C</TableCell>
                </TableRow>
                <TableRow>
                    <TableCell>RD</TableCell>
                    <TableCell>Data 2B</TableCell>
                    <TableCell>Data 2C</TableCell>
                </TableRow>
            </TableBody>
        </Table>

        <Card className="mt-4 w-1/3 ml-auto">
            <CardContent className="p-4">
                <div className="flex items-center justify-between">
                    <span className="font-semibold text-sm">Total:</span>
                    <span className="text-lg font-bold">{total}</span>
                </div>
            </CardContent>
        </Card>
    </section>
    )

}