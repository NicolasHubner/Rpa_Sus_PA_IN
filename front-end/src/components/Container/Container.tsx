import React from "react";

export const Container = ({ children }: Readonly<{ children: React.ReactNode }>) => {
    return (
        <div className="container mx-auto px-4 bg-gray-100">
        {children}
        </div>
    )
}