//
//  AMQPConsumerThread.m
//  Objective-C wrapper for librabbitmq-c
//
//  Copyright 2009 Max Wolter. All rights reserved.
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
//
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.
//

#import "AMQPConsumerThread.h"

#import "AMQPConsumer.h"
#import "AMQPMessage.h"

@implementation AMQPConsumerThread{
    NSString *nameThread;

}

@synthesize delegate;

- (id)initWithConsumer:(AMQPConsumer *)theConsumer delegate:(NSObject <AMQPConsumerThreadDelegate> *)deleGate nameThread:(NSString *)name {
    if(self = [super init])
	{
		consumer = theConsumer;
        nameThread=name;
        [self setDelegate:deleGate];
	}
	return self;
}

- (void)main
{
    [[NSThread currentThread] setName:nameThread];
    if ([self isCancelled]) {
        NSLog(@"Thread %@ is cancel.", [NSThread currentThread]);
    }
    int countTry=3;
    while(![self isCancelled])
	{
        AMQPMessage *message = [consumer pop];
		if(message) {
            [delegate performSelector:@selector(amqpConsumerThreadReceivedNewMessage:) withObject:message];
        }else {
            countTry--;
            if (countTry==0){

                NSLog(@"Consumer lose Connection: %@",[NSThread currentThread]);
//                [delegate performSelector:@selector(amqpConsumerThreadLoseConnection) withObject:nil];
                [NSThread exit];
            }
            NSLog(@"NO messsage for consumer: %@",[NSThread currentThread]);
            [NSThread sleepForTimeInterval:5];
        }
	}
}

@end
