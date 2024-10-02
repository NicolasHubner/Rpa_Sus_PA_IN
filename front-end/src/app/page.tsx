'use client';

import { Input } from "@/components/ui/input"
import {Container} from "@/components/Container/Container";
import React from "react";




const LoginForm = () => {
    const emailRef = React.useRef<HTMLInputElement>(null);
    const passwordRef = React.useRef<HTMLInputElement>(null);

    const handleSubmit = (e: React.FormEvent<HTMLFormElement>) => {
        e.preventDefault(); // Prevent the default form submission
        const email = emailRef.current?.value; // Access email value
        const password = passwordRef.current?.value; // Access password value

        console.log("submit");
        console.log('email', email);
        console.log('password', password);

        // Here you can add your login logic
    };
    return (
        <div className="flex items-center justify-center min-h-screen bg-gray-100">
            <div className="bg-white shadow-lg rounded-lg max-w-md px-12 py-8 w-full">
                <h2 className="text-3xl font-bold text-center mb-8">Login</h2>
                <form onSubmit={
                    handleSubmit
                }>
                    <div className="mb-6">
                        <label htmlFor="email" className="block text-lg font-medium text-gray-700">Email</label>
                        <Input
                            type="email"
                            id="email"
                            ref={emailRef}
                            required
                            className="mt-2 p-3 block w-full border border-gray-300 rounded-md focus:outline-none focus:ring focus:ring-blue-300 text-lg"
                            placeholder="Enter your email"
                        />
                    </div>
                    <div className="mb-8">
                        <label htmlFor="password" className="block text-lg font-medium text-gray-700">Password</label>
                        <Input
                            type="password"
                            id="password"
                            ref={passwordRef}
                            required
                            className="mt-2 p-3 block w-full border border-gray-300 rounded-md focus:outline-none focus:ring focus:ring-blue-300 text-lg"
                            placeholder="Enter your password"
                        />
                    </div>
                    <button
                        type="submit"
                        className="w-full bg-blue-600 text-white py-3 rounded-md hover:bg-blue-700 focus:outline-none focus:ring focus:ring-blue-300 text-lg"
                    >
                        Enter
                    </button>
                </form>
            </div>
        </div>
    );
};


export default function Home() {
  return (
    <Container>
      <LoginForm />
    </Container>
  );
}
