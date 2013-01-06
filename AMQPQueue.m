//
//  AMQPQueue.m
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

#import "AMQPQueue.h"
#import "amqp_framing.h"

#import "AMQPChannel.h"
#import "AMQPExchange.h"

@implementation AMQPQueue

@synthesize internalQueue = queueName;

- (id)initWithName:(NSString*)theName onChannel:(AMQPChannel*)theChannel isPassive:(BOOL)passive isExclusive:(BOOL)exclusive isDurable:(BOOL)durable getsAutoDeleted:(BOOL)autoDelete error:(NSError **)error
{
	if(self = [super init])
	{
        if ((theChannel).connection == nil) {
            NSMutableDictionary *errorDetail = [NSMutableDictionary dictionary];
            [errorDetail setValue:[NSString stringWithFormat:@"Failed Connection is Lost"] forKey:NSLocalizedDescriptionKey];
            *error = [NSError errorWithDomain:NSStringFromClass([self class]) code:-14 userInfo:errorDetail];
            return false;
        }
        amqp_queue_declare_ok_t *declaration = amqp_queue_declare((theChannel).connection.internalConnection, (theChannel).internalChannel, amqp_cstring_bytes([theName UTF8String]), passive, durable, exclusive, autoDelete, AMQP_EMPTY_TABLE);

        if([channel.connection checkLastOperation:@"Failed to declare queue"]||declaration==nil){
			NSMutableDictionary *errorDetail = [NSMutableDictionary dictionary];
			[errorDetail setValue:@"Failed to declare queue" forKey:NSLocalizedDescriptionKey];
			*error = [NSError errorWithDomain:NSStringFromClass([self class]) code:-8 userInfo:errorDetail];
			return nil;
		}
		queueName = amqp_bytes_malloc_dup(declaration->queue);
		channel =theChannel;
	}
	
	return self;
}
-(id)initWithName:(NSString *)theName onChannel:(AMQPChannel *)theChannel{
    if(self = [super init])
	{
        queueName = amqp_bytes_malloc_dup(amqp_cstring_bytes([theName UTF8String]));
		channel = theChannel;
	}
	
	return self;
}
- (void)dealloc
{
	amqp_bytes_free(queueName);
}

- (BOOL)bindToExchange:(AMQPExchange*)theExchange withKey:(NSString*)bindingKey error:(NSError **)error
{
    if (channel.connection == nil) {
        NSMutableDictionary *errorDetail = [NSMutableDictionary dictionary];
        [errorDetail setValue:[NSString stringWithFormat:@"Failed Connection is Lost"] forKey:NSLocalizedDescriptionKey];
        *error = [NSError errorWithDomain:NSStringFromClass([self class]) code:-14 userInfo:errorDetail];
        return false;
    }
	amqp_queue_bind(channel.connection.internalConnection, channel.internalChannel, queueName, theExchange.internalExchange, amqp_cstring_bytes([bindingKey UTF8String]), AMQP_EMPTY_TABLE);
	if([channel.connection checkLastOperation:@"Failed to bind queue to exchange"]){
		NSMutableDictionary *errorDetail = [NSMutableDictionary dictionary];
		[errorDetail setValue:@"Failed to bind queue to exchange" forKey:NSLocalizedDescriptionKey];
		*error = [NSError errorWithDomain:NSStringFromClass([self class]) code:-7 userInfo:errorDetail];
		return false;
	}
	return true;
}
- (BOOL)unbindFromExchange:(AMQPExchange*)theExchange withKey:(NSString*)bindingKey error:(NSError **)error
{
    if (channel.connection == nil) {
        NSMutableDictionary *errorDetail = [NSMutableDictionary dictionary];
        [errorDetail setValue:[NSString stringWithFormat:@"Failed Connection is Lost"] forKey:NSLocalizedDescriptionKey];
        *error = [NSError errorWithDomain:NSStringFromClass([self class]) code:-14 userInfo:errorDetail];
        return false;
    }
	amqp_queue_unbind(channel.connection.internalConnection, channel.internalChannel, queueName, theExchange.internalExchange, amqp_cstring_bytes([bindingKey UTF8String]), AMQP_EMPTY_TABLE);
	if([channel.connection checkLastOperation:@"Failed to unbind queue from exchange"]){
		NSMutableDictionary *errorDetail = [NSMutableDictionary dictionary];
		[errorDetail setValue:@"Failed to unbind queue from exchange" forKey:NSLocalizedDescriptionKey];
		*error = [NSError errorWithDomain:NSStringFromClass([self class]) code:-6 userInfo:errorDetail];
		return false;
	}
	return true;
}
-(void)destroy{
    amqp_queue_delete(channel.connection.internalConnection, channel.internalChannel, queueName, 0, 0);
}

@end
