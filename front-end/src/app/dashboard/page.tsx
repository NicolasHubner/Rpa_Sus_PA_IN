"use client"

import React from "react";
import {SearchStatePaRd} from "@/app/dashboard/___components/search-state-pa-rd";
import {TableQuickSearch} from "@/app/dashboard/___components/table-quick-search";
import {FileUpload} from "@/app/dashboard/___components/file-upload";

export default function Component() {
    return (
        <div className="container mx-auto p-4 space-y-8">
            {/* Section 1: File Upload */}
            <FileUpload />

            {/* Section 2: Table with Search */}
            <TableQuickSearch />

            <SearchStatePaRd />
        </div>
    )
}